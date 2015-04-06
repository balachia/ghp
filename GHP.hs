{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module GHP where

import Data.Aeson
import Data.Text (Text)
import Data.HashMap.Strict (keys, unionWith, fromList, empty, HashMap)
import Data.Maybe (isJust, fromJust)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (mzero, mapM)
import Text.Printf (printf)
import System.Directory (getHomeDirectory)
import qualified Data.ByteString.Lazy.Char8 as B

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
        readLines path = (B.readFile path) >>= (\x -> return $ B.lines x)

-- decode each entry (each line in each file)
decodeFiles :: FromJSON a => [FilePath] -> IO [Maybe a]
decodeFiles paths = (map decode) <$> (loadFiles paths)

encodeOutput :: ToJSON a => FilePath -> [Maybe a] -> IO ()
encodeOutput outfile jsons = B.writeFile outfile outstr
    where
        outstr = B.unlines ((map encode') . (filter isJust) $ jsons)
        encode' Nothing = B.empty
        encode' (Just x) = encode x

scanKeys :: FromJSON a => [Maybe a] -> HashMap Text Int
scanKeys objs' = foldr addKeys empty (map (keys . fromJust) $ filter (isJust) objs')
    where
        addKeys :: [Text] -> (HashMap Text Int) -> (HashMap Text Int)
        addKeys keys counter = unionWith (+) (fromList $ zip keys (repeat 1)) counter

{-filterOnKeys-}

