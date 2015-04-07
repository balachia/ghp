{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module GHP where

import Data.Aeson
import Data.Text (Text)
import Data.HashMap.Strict (keys, unionWith, toList, fromList, fromListWith, empty, HashMap)
import qualified Data.HashSet as HS
import Data.Maybe (isJust, fromJust)
import Data.Foldable (foldl')
import Control.Applicative ((<$>), (<*>))
import Control.Monad (mzero, mapM)
import Text.Printf (printf)
import System.Directory (getHomeDirectory)
import qualified System.IO as IO
import qualified System.IO.Strict as IOS
import qualified Data.ByteString.Lazy.Char8 as B
{-import qualified Data.ByteString.Char8 as B-}

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
makePaths base range = map (printf base) range

basepath post = do
    home <- getHomeDirectory
    return (home ++ "/" ++ post)
    {-return (home ++ "/Data/github/user-repos/%0.8d.json")-}

-- each file contains a bunch of lines
-- take each line from each file, and bunch them all together
loadFiles :: [FilePath] -> IO [B.ByteString]
loadFiles paths = concat <$> (mapM (readLines) paths)
    where
        readLines :: FilePath -> IO [B.ByteString]
        readLines path = (IOS.readFile path) >>= (\x -> return $ B.lines (B.pack x))
        {-readLines path = (B.readFile path) >>= (\x -> return $ B.lines x)-}

loadFiles' :: [FilePath] -> [IO [B.ByteString]]
loadFiles' paths = map readLines paths
    where
        readLines path = (IOS.readFile path) >>= (\x -> return $ (tail . B.lines) (B.pack x))

loadFilesWithPath :: [FilePath] -> [IO [(B.ByteString,B.ByteString)]]
loadFilesWithPath paths = map readLines paths
    where
        readLines path = (IOS.readFile path) >>= (\x -> return $ [(x, B.pack path) | x <- (tail . B.lines) (B.pack x)])

-- decode each entry (each line in each file)
decodeFiles :: FromJSON a => [FilePath] -> IO [Maybe a]
decodeFiles paths = (map decode) <$> (loadFiles paths)

decodeFiles' :: FromJSON a => [FilePath] -> [IO [Maybe a]]
{-decodeFiles' :: FromJSON a => [FilePath] -> [IO [Either String a]]-}
decodeFiles' paths = insertFcn decode (loadFiles' paths)
{-decodeFiles' paths = insertFcn eitherDecode (loadFiles' paths)-}

decodeFilesErr :: FromJSON a => [FilePath] -> [IO [Either B.ByteString a]]
decodeFilesErr paths = insertFcn decode' (loadFilesWithPath paths)
    where
        decode' :: FromJSON a => (B.ByteString, B.ByteString) -> Either B.ByteString a
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

{-traceIO :: Int -> [IO a] -> [IO a]-}
{-traceIO n acts = traceIO' n 1 1 acts-}
    {-where-}
        {-traceIO' :: Int -> Int -> Int -> [IO a] -> [IO a]-}
        {-traceIO' _ _ _ [] = []-}
        {-traceIO' n k tot (x:[]) = [doThenTrace x tot]-}
        {-traceIO' n k tot (x:xs) | k == n = (doThenTrace x tot):(traceIO' n 1 (tot+1) xs)-}
                                {-| otherwise = x:(traceIO' n (k+1) (tot+1) xs)-}
        {-doThenTrace x tot = x >>= \ret -> ((putStrLn (show tot)) >> return ret)-}

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


{-filterOnKeys-}

