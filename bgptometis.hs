{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExtendedDefaultRules #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
import Shelly
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.IO as TIO
import Control.Monad
import System.Directory
import Data.Time ( fromGregorian, dayOfWeek, DayOfWeek(Tuesday) )
import Data.List.Split
import Data.IORef
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Data.Int
import Data.Char
import System.ProgressBar
import Control.Applicative
import System.Environment
import System.FilePath
import Data.List
import Control.DeepSeq (force)
default (T.Text)

type ASN = Int32
type ASConnections = Map.Map ASN (Map.Map ASN Int)

type IPRange  = T.Text
type IPRanges = Set.Set IPRange

type ASNData = Map.Map ASN IPRanges

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
ipRangeWeight = sum . Set.map ipRangeToInt

retrieveIPWeight:: Ord k => k -> Map.Map k IPRanges -> Int
retrieveIPWeight k mymap = ipRangeWeight $ Map.findWithDefault Set.empty k mymap

openBGPFile :: FilePath -> IO T.Text
openBGPFile file = 
    let cmdArg = silently (cmd "bgpdump" ["-q", "-M", toTextIgnore file])
        in shelly cmdArg

isBZ2File :: FilePath -> Bool
isBZ2File str = last (splitOn "." str) == "bz2"

isRIBSFile :: FilePath -> Bool
isRIBSFile file = let
    sections = splitOn "." (last (splitOn [pathSeparator] file))     in
    head sections == "rib" && all isDigit (sections !! 1) && all isDigit (sections !! 2) && sections !! 3 == "bz2"

getDay :: FilePath -> DayOfWeek 
getDay file = let
    dateStr = splitOn "." (takeFileName file) !! 1
    year  = read (take 4 dateStr) 
    month = read (take 2 (drop 4 dateStr)) 
    day   = read (take 2 (drop 6 dateStr)) 
    in dayOfWeek (fromGregorian year month day)

isTuesday :: FilePath -> Bool
isTuesday file = getDay file == Tuesday

takeSameDay :: [FilePath] -> [FilePath]
takeSameDay []          = []
takeSameDay (file:rest) = file:takeWhile ((getDay file ==) . getDay) rest


combineIPRanges :: IPRanges -> IO IPRanges
combineIPRanges ranges = 
    let cmdArg = map (T.dropWhile isSpace) . T.lines <$> silently (run "netmask" (Set.toList ranges))
        in Set.fromList <$> shelly cmdArg

unionASConnections :: ASConnections -> ASConnections -> ASConnections
unionASConnections = Map.unionWith (Map.unionWith (force (+)))

formatASConnections :: Map.Map ASN Int -> String
formatASConnections asmap = unwords (map (\(x,y) -> show x ++ ' ':show y) (Map.toAscList asmap))

getSubdirectories :: FilePath -> IO [FilePath]
getSubdirectories ofPath = let
    handleCurrentDir = listDirectory ofPath >>= mapM makeAbsolute
    handleRecursion = do
        currentContents <- handleCurrentDir
        currentDirs <- filterM doesDirectoryExist currentContents
        concat <$> mapM (\x -> setCurrentDirectory x >> getSubdirectories x) currentDirs
    in liftA2 (<>) handleCurrentDir handleRecursion
    
getRIBS :: IO [FilePath]
getRIBS = filter isRIBSFile <$> (getCurrentDirectory >>= \directory -> getSubdirectories directory <* setCurrentDirectory directory)

ribsMonthYear :: String -> String -> FilePath -> Bool
ribsMonthYear month year file = take 6 (splitOn "." (takeFileName file) !! 1) == year ++ month

firstTuesday :: String -> String -> [FilePath] -> [FilePath]
firstTuesday month year = filter ((&&) <$> isTuesday <*> ribsMonthYear month year)

processOneFile :: IORef ASNData -> IORef ASConnections -> [T.Text] -> IO ()
processOneFile asnMap conMap filetext = do
    pb <- newProgressBar defStyle 10 (Progress 0 (length filetext) ())
    forM_ filetext $ \line -> do
        let ((asn, ipdata), newConnections) = parseLine line
        modifyIORef' asnMap (Map.insertWith Set.union asn (Set.singleton ipdata))
        modifyIORef' conMap (unionASConnections newConnections)
        incProgress pb 1


processApproved :: IORef ASNData -> IORef ASConnections -> FilePath -> IO ()
processApproved asnMap conMap filename = do 
    putStrLn ("Opening BGP file " ++ filename ++ " using bgpdump")
    filetext <- T.lines <$> openBGPFile filename
    putStr "Convering file " >> putStr filename >> putStrLn " to map" 
    processOneFile asnMap conMap filetext
    putStrLn "Squashing ipdata using netmask..."
    rawASNMap <- readIORef asnMap
    pb <- newProgressBar defStyle 10 (Progress 0 (Map.size rawASNMap) ())
    combinedASN <- mapM (combineIPRanges >=> (\x -> incProgress pb 1 >> return x)) rawASNMap
    writeIORef asnMap combinedASN

test :: FilePath -> IO ()
test filepath = do
    asnMap <- newIORef Map.empty
    conMap <- newIORef Map.empty
    input  <- T.lines <$> TIO.readFile filepath
    processOneFile asnMap conMap input
    conMapFinal <- readIORef conMap
    asnMapFinal <- readIORef asnMap 
    outputFile asnMapFinal conMapFinal "12" "2035"
    return ()
    

outputFile :: ASNData -> ASConnections -> String -> String -> IO ()
outputFile asnMapFinal conMapFinal month year = do
    let n            = length . Map.keys  $ conMapFinal
        m            = length . concatMap Map.elems . Map.elems $ conMapFinal
        outputName   = year ++ "." ++ month
        outMetis     = outputName ++ ".metis"
        outNodemap   = outputName ++ ".nodemap"
        conMapLst    = Map.toAscList conMapFinal
        headerString = show n ++ ' ':show m ++ " 011\n"
    putStrLn (show n ++ " nodes found")
    putStrLn (show m ++ " edges found")
    writeFile outMetis headerString
    writeFile outNodemap ""
    putStrLn "Writing output"
    pb <- newProgressBar defStyle 10 (Progress 0 (length conMapLst) ())
    forM_ conMapLst $ \(asnNum, conweights) -> do
        appendFile outMetis (show (retrieveIPWeight asnNum asnMapFinal) ++ " " ++ formatASConnections conweights ++ "\n")
        appendFile outNodemap $ show asnNum ++ "\n"
        incProgress pb 1

bgpDumpMonthYear :: String -> String -> IO ()
bgpDumpMonthYear month year = do
    approvedFiles <- firstTuesday month year <$> getRIBS 
    when (null approvedFiles) (putStrLn "No files found.")
    guard (not $ null approvedFiles)
    putStrLn "Files found:"
    mapM_ putStrLn approvedFiles
    asnMap <- newIORef Map.empty
    conMap <- newIORef Map.empty
    forM_ approvedFiles (processApproved asnMap conMap)
    conMapFinal <- readIORef conMap
    asnMapFinal <- readIORef asnMap 
    outputFile asnMapFinal conMapFinal month year

usage :: String
usage = "bgptometis month year while in the bgpdata file, currently does not recursively search for files."

main :: IO ()
main = do
    args   <- getArgs
    case args of
        [] -> do
            putStrLn usage
        (month:year:xs) -> do
            bgpDumpMonthYear month year