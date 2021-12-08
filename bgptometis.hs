{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
import Shelly
import qualified Data.Text as T
import Control.Monad
import System.Directory
import Data.Time
import Data.List.Split
import Data.IORef
import qualified Data.Map as Map
import Data.Int
import Data.Char
import System.ProgressBar
import qualified Data.IntMap as T
import System.Environment
default (T.Text)

type ASN = Int32
type ASConnections = Map.Map ASN (Map.Map ASN Int)

type IPRange  = T.Text
type IPRanges = [IPRange]

type ASNData = Map.Map Int IPRanges

addASNToMap :: ASNData -> (ASN, IPRange) -> IO ASNData
addASNToMap oldmap (key, val) = undefined

parseLine :: T.Text -> ((ASN, T.Text), ASConnections)
parseLine str = let
    splitData = T.splitOn "|" str
    ipRange   = splitData !! 5
    asnList   = map (read . T.unpack) $ takeWhile (T.all isDigit) (T.words (splitData !! 6))
    conMap    = Map.fromList . zip asnList $ zipWith Map.singleton (tail asnList) [1, 1..]
    in ((last asnList, ipRange),  conMap)

ipRangeToInt :: IPRange -> Int
ipRangeToInt range = 2 ^ (32 - read (T.unpack (T.splitOn "/" range !! 1)) :: Int)

ipRangeWeight :: IPRanges -> Int
ipRangeWeight = sum . map ipRangeToInt

retrieveIPWeight:: Ord k => k -> Map.Map k IPRanges -> Int
retrieveIPWeight k mymap = ipRangeWeight $ Map.findWithDefault [] k mymap

openBGPFile :: FilePath -> IO T.Text
openBGPFile file = 
    let cmdArg = silently $ cmd "bgpdump" ["-q", "-M", T.pack file]
        in shelly cmdArg

isBZ2File :: FilePath -> Bool
isBZ2File str = last (splitOn "." str) == "bz2"

isTuesday :: FilePath -> Bool
isTuesday str = let 
    dateStr = splitOn "." (last $ splitOn "/" str) !! 1
    year  = read (take 4 dateStr) 
    month = read (take 2 (drop 4 dateStr)) 
    day   = read (take 2 (drop 6 dateStr)) 
    in
        Tuesday == dayOfWeek (fromGregorian year month day)

testLine :: T.Text
testLine = "TABLE_DUMP|10/27/01 05:48:26|B|209.244.2.115|3356|3.0.0.0/8|3356 701 80|IGP"

takeSameDay :: [FilePath] -> [FilePath]
takeSameDay []          = []
takeSameDay (file:rest) = file:takeWhile isSameDay rest
    where
        isSameDay x = splitOn "." file !! 1 == splitOn "." x !! 1 

combineIPRanges :: IPRanges -> IO IPRanges
combineIPRanges ranges = --undefined
    let cmdArg = map (T.dropWhile isSpace) . T.lines <$> silently (run "netmask" ranges)
        in shelly cmdArg

unionASConnections :: ASConnections -> ASConnections -> ASConnections
unionASConnections = Map.unionWith (Map.unionWith (+))

formatASConnections :: Map.Map ASN Int -> String
formatASConnections asmap = unwords (map (\(x,y) -> show x ++ ' ':show y) (Map.toAscList asmap))

getFiles :: String -> String -> IO [FilePath]
getFiles month year = 
    getCurrentDirectory >>= \current -> 
        map (\x -> current ++ '/':year ++ '.':month ++ "/RIBS/" ++ x) <$> 
            getDirectoryContents (current ++ '/':year ++ '.':month ++ "/RIBS")

bgpDumpMonthYear :: String -> String -> IO ()
bgpDumpMonthYear month year = do
    let fileQuals = takeSameDay . filter (\x -> isBZ2File x && isTuesday x)
    approvedFiles <- fileQuals <$> getFiles month year
    asnMap <- newIORef Map.empty
    conMap <- newIORef Map.empty
    forM_ approvedFiles $ \filename -> do 
        putStrLn ("Opening BGP file " ++ filename ++ " using bgpdump")
        filetext <- T.lines <$> openBGPFile filename
        putStr "Convering file " >> putStr filename >> putStrLn " to map" 
        pb <- newProgressBar defStyle 10 (Progress 0 (length filetext) ())
        forM_ filetext $ \line -> do
            let ((asn, ipdata), newConnections) = parseLine line
            modifyIORef' asnMap (Map.insertWith (++) asn [ipdata])
            modifyIORef' conMap (unionASConnections newConnections)
            incProgress pb 1
        putStrLn "Squashing ipdata using netmask..."
        rawASNMap <- readIORef asnMap
        pb <- newProgressBar defStyle 10 (Progress 0 (Map.size rawASNMap) ())
        combinedASN <- mapM (combineIPRanges >=> (\x -> incProgress pb 1 >> return x)) rawASNMap
        writeIORef asnMap combinedASN
    conMapFinal <- readIORef conMap
    asnMapFinal <- readIORef asnMap 
    let n = length . Map.keys  $ conMapFinal
    putStrLn (show n ++ " nodes found")
    let m = length . concatMap Map.elems . Map.elems $ conMapFinal
    putStrLn (show m ++ " edges found")
    let outputName = "2001.10"
    let outMetis   = outputName ++ "metis"
    let outNodemap = outputName ++ ".nodemap"
    let conMapLst = Map.toAscList conMapFinal
    let headerString = show n ++ ' ':show m ++ " 011"
    writeFile outMetis   headerString
    writeFile outNodemap ""
    putStrLn "Writing output"
    pb <- newProgressBar defStyle 10 (Progress 0 (length conMapLst) ())
    forM_ conMapLst $ \(asnNum, asnweights) -> do
        appendFile outMetis ('\n':show (retrieveIPWeight asnNum asnMapFinal)  ++ formatASConnections asnweights)
        appendFile outNodemap $ show asnNum ++ "\n"
        incProgress pb 1

usage :: String
usage = "bgptometis month year while in the bgpdata file, currently does not recursively search for files."

main :: IO ()
main = do
    args   <- getArgs
    case args of
        (month:year:xs) -> do
            bgpDumpMonthYear month year