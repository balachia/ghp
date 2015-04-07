{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

import Data.Aeson
import Data.Aeson.TH
import Data.Text (pack, Text)
import GHP
import Control.Applicative ((<$>), (<*>))
import Control.Monad (mzero, mapM)
import System.IO
import Control.Concurrent.ParallelIO.Local
import qualified Data.ByteString.Lazy.Char8 as B

repoFields :: [Text]
repoFields = map pack fields
    where
        fields = [ 
                "has_wiki","updated_at","private","full_name",
                "id","size","has_downloads","has_pages",
                "watchers_count","stargazers_count","permissions","homepage",
                "fork","description","forks","has_issues",
                "open_issues_count","watchers","name","language",
                "url","created_at","pushed_at","forks_count",
                "default_branch","open_issues"
            ]

data Repo = Repo
    { f_id :: Int
    , f_owner :: User
    , f_name :: String
    , f_full_name :: String
    , f_description :: Maybe String
    , f_private :: Bool
    , f_fork :: Bool
    , f_url :: String
    , f_homepage :: Maybe String
    , f_language :: Maybe String
    , f_forks_count :: Int
    , f_stargazers_count :: Int
    , f_watchers_count :: Int
    , f_size :: Int
    , f_default_branch :: String
    , f_open_issues_count :: Int
    , f_has_issues :: Bool
    , f_has_wiki :: Bool
    , f_has_pages :: Bool
    , f_has_downloads :: Bool
    , f_pushed_at :: Maybe String
    , f_created_at :: Maybe String
    , f_updated_at :: Maybe String
    , f_watchers :: Int
    , f_forks :: Int
    , f_open_issues :: Int
    } deriving Show

$(deriveJSON defaultOptions{fieldLabelModifier = drop 2} ''Repo)

{-injsons :: IO [Maybe Object]-}
{-injsons = do-}
    {-basepath' <- basepath "Data/github/user-repos/%0.8d.json"-}
    {-let paths = makePaths basepath' [1..maxFile]-}
    {-decodeFiles paths-}
    {-where-}
        {-maxFile = 1000-}

processJSONs = do
    inpaths <- basepath "Data/github/user-repos/%0.8d.json"
    let paths = makePaths inpaths [1..maxFile]
    hdlpath <- basepath "Data/github/test-hs.json"
    errhdlpath <- basepath "Data/github/test-hs.err"
    hdl <- openFile hdlpath WriteMode
    errhdl <- openFile errhdlpath WriteMode
    let injsons = (decodeFilesErr paths :: [IO [Either B.ByteString Repo]])
    {-let injsons = (decodeFiles' paths :: [IO [Maybe Repo]])-}
    {-let injsons = (decodeFiles' paths :: [IO [Either String Repo]])-}
    {-let injsons = decodeFiles' paths :: [IO [Maybe Object]]-}
    outputEach hdl errhdl injsons
    {-outputEach hdl injsons-}
    {-outputAll hdl injsons-}
    mapM_ hClose [hdl, errhdl]
    where
        maxFile = 1200000
        {-outputEach hdl injsons = (sequence_ (traceIO 500 (writeOutput hdl injsons)))-}
        outputEach hdl errhdl injsons = (sequence_ (traceIO 500 (writeOutputErr hdl errhdl injsons)))
        {-outputAll hdl injsons = head $ writeOutput hdl [concat <$> (sequence $ traceIO 500 injsons)]-}
        outputAll hdl errhdl injsons = head $ writeOutputErr hdl errhdl [concat <$> (sequence $ traceIO 500 injsons)]

main = processJSONs


