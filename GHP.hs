{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module GHP where

import Data.Aeson
import Data.Text (Text)
import Data.HashMap.Strict (keys, unionWith, toList, fromList, fromListWith, empty, HashMap)
import qualified Data.HashSet as HS
import Data.Maybe (isJust, fromJust)
import Data.Foldable (foldl')
import Data.List (insertBy)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (mzero, mapM)
import Text.Printf (printf)
import System.Directory (getHomeDirectory)
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TBChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.ParallelIO.Local
import qualified System.IO as IO
import qualified System.IO.Strict as IOS
import qualified Data.ByteString.Lazy.Char8 as B
{-import qualified Data.ByteString.Char8 as B-}

data JSONRes = JSON B.ByteString | Err B.ByteString

data User = User 
    { login :: String
    , id :: Int
    , url :: String
    , type' :: String
    , site_admin :: Bool
    } deriving Show

instance FromJSON User where
    parseJSON (Object v) = User <$>
        v .: "login" <*>
        v .: "id" <*>
        v .: "url" <*>
        v .: "type" <*>
        v .: "site_admin"
    parseJSON _ = mzero

instance ToJSON User where
    toJSON (User login id url type' site_admin) = object ["login" .= login, "id" .= id, "url" .= url, "type" .= type', "site_admin" .= site_admin]

testUserString = B.pack "{\"login\":\"username\", \"id\":1, \"url\":\"foo\", \"type\":\"User\",\"site_admin\":true}"

makePaths :: String -> [Int] -> [String]
makePaths base range = map printf' range
    where
        {-printf' x = printf (base ++ "/%0.4d/%0.8d.json") (x `quot` 10000) x-}
        printf' x = printf base (x `quot` 10000) x
        {-printf' x = printf base x-}

basepath post = do
    home <- getHomeDirectory
    return (home ++ "/" ++ post)
    {-return (home ++ "/Data/github/user-repos/%0.8d.json")-}

-- each file contains a bunch of lines
-- take each line from each file, and bunch them all together
loadFiles :: [FilePath] -> [IO [B.ByteString]]
loadFiles paths = map readLines paths
    where
        readLines path = (IOS.readFile path) >>= (\x -> return $ (tail . B.lines) (B.pack x))

loadFilesWithPath :: [FilePath] -> [IO [(B.ByteString,B.ByteString)]]
loadFilesWithPath paths = map readLines paths
    where
        {-readLines path = (IOS.readFile path) >>= (\x -> return $ [(x, B.pack path) | x <- (tail . B.lines) (B.pack x)])-}
        {-readLines path = readFile' path >>= (\x -> return $ [(x, B.pack path) | x <- (tail . B.lines) (B.pack x)])-}
        -- is this an arrow?
        readLines path = (appendPath path) <$> (readFile' path)
        readFile' path = IOS.readFile path
        appendPath path x = [(x, B.pack path) | x <- (tail . B.lines) (B.pack x)]

decodeFiles :: FromJSON a => [FilePath] -> [IO [Maybe a]]
{-decodeFiles :: FromJSON a => [FilePath] -> [IO [Either String a]]-}
decodeFiles paths = insertFcn decode (loadFiles paths)
{-decodeFiles paths = insertFcn eitherDecode (loadFiles paths)-}

decodeFilesErr :: FromJSON a => [FilePath] -> [IO [Either B.ByteString a]]
decodeFilesErr paths = insertFcn decode' (loadFilesWithPath paths)
    where
        decode' :: FromJSON a => (B.ByteString, B.ByteString) -> Either B.ByteString a
        {-decode' (x,path) = case (decode x) of-}
        decode' (x,path) = case (decode x) of
            Just x' -> Right x'
            Nothing -> Left (B.unlines [path,x])

writeOutput :: ToJSON a => IO.Handle -> [IO [a]] -> [IO ()]
{-writeOutput hdl jsons = insertFcn (\x -> putStrLn "test") jsons-}
writeOutput hdl jsons = map encodeFile jsons
    where
        encodeFile ioxs = do
            xs <- ioxs
            sequence_ (map ((B.hPutStrLn hdl).encode) xs)

writeOutputErr :: ToJSON a => IO.Handle -> IO.Handle -> [IO [Either B.ByteString a]] -> [IO ()]
writeOutputErr hdl errhdl jsons = map encodeFile jsons
    where
        encodeFile ioxs = do
            xs <- ioxs
            sequence_ (map (splitOutput) xs)
        splitOutput (Right x) = (B.hPutStrLn hdl) $ encode x
        splitOutput (Left x) = (B.hPutStr errhdl x)

writeJSONRes :: IO.Handle -> IO.Handle -> [JSONRes] -> IO ()
writeJSONRes hdl errhdl jsons = sequence_ $ map writeJSONRes' jsons
    where
        writeJSONRes' (JSON x) = B.hPutStrLn hdl x
        writeJSONRes' (Err x) = B.hPutStrLn errhdl x

-- how to preserve ordering while writing to channel?
writeOutputTBChan :: ToJSON a => TBChan (Maybe Int, [JSONRes]) -> [IO [Either B.ByteString a]] -> [IO ()]
writeOutputTBChan chan jsons = zipWith encodeFile jsons [1..]
    where
        encodeFile ioxs count = do
            xs <- ioxs
            atomically (writeTBChan chan (Just count, (map splitOutput xs)))
        splitOutput (Right x) = JSON (encode x)
        splitOutput (Left x) = Err x

-- how to read from channel?
-- 1) check your state
--      if first value is terminator, terminate, doing whatever needed to inform parent
--      if first value is correct, write it out, loop again
-- 2) read next value from channel, insert it into list, sorted
--      loop again
writeFromTBChan :: IO.Handle -> IO.Handle -> TMVar Bool -> TBChan (Maybe Int, [JSONRes]) -> Int -> [(Maybe Int, [JSONRes])] -> IO ()
--first write with no order preservation
writeFromTBChan hdl errhdl doneflag chan next state = writeFromTBChan' next state
    where
        writeFromTBChan' :: Int -> [(Maybe Int, [JSONRes])] -> IO ()
        writeFromTBChan' next [] = do
            item <- atomically (readTBChan chan)
            case item of
                (Just i, vals) -> writeFromTBChan' next [item]
                (Nothing, _) -> endThread
        writeFromTBChan' next ((Nothing, []):xs) = do
            endThread
        writeFromTBChan' next state@((Just i, vals):xs) | i == next = do
            writeJSONRes hdl errhdl vals
            writeFromTBChan' (next+1) xs
                                                 | otherwise = do
            {-putStrLn (show $ dumpState state)-}
            item <- atomically (readTBChan chan)
            case item of
                (Nothing, _) -> error "Received End-of-Data signal with items left to write"
                (Just i, vals) -> writeFromTBChan' next (insertBy itemOrder item state)
        dumpState state = map (\(Just x, _) -> x) state
        itemOrder (Just x, _) (Just y, _) = compare x y
        endThread = do
            atomically $ putTMVar doneflag True
            putStrLn "Writer Done"


-- takes a function f, receiving arguments: intersperse gap, iterations since last intersperse, total iterations
intersperseIO :: (Int -> Int -> Int -> IO ()) -> Int -> [IO a] -> [IO a]
intersperseIO f n acts = intersperseIO' f n 1 1 acts
    where
        intersperseIO' :: (Int -> Int -> Int -> IO ()) -> Int -> Int -> Int -> [IO a] -> [IO a]
        intersperseIO' _ _ _ _ [] = []
        intersperseIO' f n k tot (x:[]) = [xThenF f n k tot x]
        intersperseIO' f n k tot (x:xs) | k == n = (xThenF f n k tot x):(intersperseIO' f n 1 (tot+1) xs)
                                        | otherwise = x:(intersperseIO' f n (k+1) (tot+1) xs)
        xThenF f n k tot x = x >>= \ret -> ((f n k tot) >> return ret)

traceIO = intersperseIO (\n k tot -> putStrLn (show tot))

insertFcn :: (a -> b) -> [IO [a]] -> [IO [b]]
insertFcn f as = map (fmap (map f)) as

-- write a bunch of decoded jsons back out to a single file
encodeOutput :: ToJSON a => FilePath -> [Maybe a] -> IO ()
encodeOutput outfile jsons = B.writeFile outfile outstr
    where
        outstr = B.unlines ((map encode') . (filter isJust) $ jsons)
        encode' Nothing = B.empty
        encode' (Just x) = encode x

-- compile keys observed in jsons
{-scanKeys :: [Maybe Object] -> HashMap Text Int-}
scanKeys :: [Maybe Object] -> HashMap Text Int
scanKeys objs = foldl' keyFold empty (objKeys justObjs)
    where
        justObjs = map fromJust $ filter (isJust) objs
        objKeys xs = map keys xs
        keyFold :: HashMap Text Int -> [Text] -> HashMap Text Int
        keyFold counter xs = unionWith (+) counter (fromList $ zip xs (repeat 1))

-- count the number of times each key is used
invertKeys :: HashMap Text Int -> HashMap Int [Text]
invertKeys kvs = fromListWith (++) [(y,[x]) | (x,y) <- (toList kvs)]

isSubsetOf :: [Text] -> [Text] -> Bool
isSubsetOf as bs = HS.null $ (HS.fromList as) `HS.difference` (HS.fromList bs)

-- IO

processJSONs :: ToJSON a => ([FilePath] -> [IO [Either B.ByteString a]]) -> String -> String -> Int -> IO ()
processJSONs injsonsf infiles outfile maxFile = do
    {-inpaths <- basepath infiles-}
    inpaths <- (++ "/%0.4d/%0.8d.json") <$> (basepath infiles)
    {-let inpaths = inpaths' ++ "/%0.4d/%0.8d.json"-}
    let paths = makePaths inpaths [1..maxFile]
    hdlpath <- basepath (outfile ++ ".json")
    errhdlpath <- basepath (outfile ++ ".err")
    hdl <- IO.openFile hdlpath IO.WriteMode
    errhdl <- IO.openFile errhdlpath IO.WriteMode
    {-let injsons = (decodeFilesErr f paths)-}
    outputEach hdl errhdl (injsonsf paths)
    mapM_ IO.hClose [hdl, errhdl]

parallelProcessJSONs :: ToJSON a => ([FilePath] -> [IO [Either B.ByteString a]]) -> String -> String -> Int -> IO ()
parallelProcessJSONs injsonsf infiles outfile maxFile = do
    {-inpaths <- basepath infiles-}
    inpaths <- (++ "/%0.4d/%0.8d.json") <$> (basepath infiles)
    {-let inpaths = inpaths' ++ "/%0.4d/%0.8d.json"-}
    let paths = makePaths inpaths [1..maxFile]
    hdlpath <- basepath (outfile ++ ".json")
    errhdlpath <- basepath (outfile ++ ".err")
    hdl <- IO.openFile hdlpath IO.WriteMode
    errhdl <- IO.openFile errhdlpath IO.WriteMode
    {-IO.hSetBuffering hdl (IO.BlockBuffering (Just 10000))-}
    -- make channels and flags
    writechan <- atomically $ (newTBChan 10000 :: STM (TBChan (Maybe Int, [JSONRes])))
    doneflag <- atomically $ (newEmptyTMVar :: STM (TMVar Bool))
    {-forkIO $ (threadDelay 50000000 >> putStrLn "Starting Writer" >> threadDelay 2000000 >> writeFromTBChan hdl errhdl doneflag writechan 1 [])-}
    forkIO $ writeFromTBChan hdl errhdl doneflag writechan 1 []
    {-let injsons = (decodeFilesErr f paths)-}
    parallelOutputEach 20 writechan (injsonsf paths)
    {-threadDelay 5000000-}
    atomically $ writeTBChan writechan (Nothing, [])
    atomically $ takeTMVar doneflag
    mapM_ IO.hClose [hdl, errhdl]

-- tracing is an odd notion with parallel processing
-- maybe move the trace to the writer process?
parallelOutputEach :: ToJSON a => Int -> TBChan (Maybe Int, [JSONRes]) -> [IO [Either B.ByteString a]] -> IO ()
parallelOutputEach threads chan injsons = withPool threads (\pool -> parallel_ pool acts)
    where
        {-acts = traceIO 500 (writeOutputTBChan chan injsons)-}
        acts = intersperseIO traceChan 500 (traceIO 500 (writeOutputTBChan chan injsons))
        traceChan n k tot = do
            {-free <- atomically $ estimateFreeSlotsTBChan chan-}
            free <- atomically $ freeSlotsTBChan chan
            putStrLn ((show free) ++ " free")

outputEach :: ToJSON a => IO.Handle -> IO.Handle -> [IO [Either B.ByteString a]] -> IO ()
outputEach hdl errhdl injsons = (sequence_ (traceIO 500 (writeOutputErr hdl errhdl injsons)))

outputAll :: ToJSON a => IO.Handle -> IO.Handle -> [IO [Either B.ByteString a]] -> IO ()
outputAll hdl errhdl injsons = head $ writeOutputErr hdl errhdl [concat <$> (sequence $ traceIO 500 injsons)]


