import qualified Data.Map as Map
import qualified Data.Map.Strict as Map.Strict
import System.Environment
import Data.IORef
import qualified Data.Set as Set
import System.Command
import Data.List.Split
import System.Directory
import Data.Foldable
import Data.Traversable
import Control.Monad
import Data.Char
import System.ProgressBar
import Data.Time
import Data.Bifunctor

type ASConnections = Map.Map Int (Map.Map Int Int)

type ASNData = Map.Map Int IPRanges

type IPRange  = String 
type IPRanges = [IPRange]

getWeight :: Ord k => k -> Map.Map k Int -> Int
getWeight = Map.findWithDefault 0


addWeight :: Ord k => k -> Int -> Map.Map k Int -> Map.Map k Int
addWeight = Map.Strict.insertWith (+)


parseLine :: String -> (ASNData, ASConnections)
parseLine str = let
    splitData = splitOn "|" str
    ipRange   = splitData !! 5
    asnList   = read <$> filter (all isDigit) (words (splitData !! 6))
    in (Map.singleton (last asnList) [ipRange], Map.map (`Map.singleton` 1) $ Map.fromList $ zip asnList (tail asnList))

unionASNData :: ASNData -> ASNData -> ASNData
unionASNData  = Map.unionWith  (++)

unionsASNData :: [ASNData] -> ASNData
unionsASNData = Map.unionsWith (++)

unionASConnections :: ASConnections -> ASConnections -> ASConnections
unionASConnections = Map.unionWith (Map.unionWith (+))

unionsASConnections :: [ASConnections] -> ASConnections
unionsASConnections = Map.unionsWith (Map.unionWith (+))

handleSingleFile :: String -> IO (ASNData, ASConnections)
handleSingleFile filename = do
    input <- lines <$> openBGPFile filename
    let parsedLines   = map parseLine input
    let asnMaps       = unionsASNData       $ fst <$> parsedLines
    let asConMaps     = unionsASConnections $ snd <$> parsedLines
    return (asnMaps, asConMaps)

formatASConnections :: Map.Map Int Int -> String
formatASConnections asmap = unwords (map (\(x,y) -> show x ++ ' ':show y) (Map.toAscList asmap))

getFiles :: String -> String -> IO [FilePath]
getFiles month year = 
    getCurrentDirectory >>= \current -> 
        map (\x -> current ++ '/':year ++ '.':month ++ "/RIBS/" ++ x) <$> 
            getDirectoryContents (current ++ '/':year ++ '.':month ++ "/RIBS")

isTextFile :: FilePath -> Bool
isTextFile str = last (splitOn "." str) == "txt"

isBZ2File :: FilePath -> Bool
isBZ2File str = last (splitOn "." str) == "bz2"

combineIPRanges :: IPRanges -> IO IPRanges
combineIPRanges ranges = do
    Stdout out <- cmd "netmask -c" ranges
    return (map (dropWhile isSpace) . lines $ out)

openBGPFile :: FilePath -> IO String
openBGPFile filepath = do
    Stdout out <- cmd ("bgpdump -M " ++ filepath)
    return out

ipRangeToInt :: IPRange -> Int
ipRangeToInt range = 2 ^ (32 - read (splitOn "/" range !! 1))

ipRangeWeight :: IPRanges -> Int
ipRangeWeight = sum . map ipRangeToInt

retrieveIPWeight:: Ord k => k -> Map.Map k IPRanges -> Int
retrieveIPWeight k mymap = ipRangeWeight $ Map.findWithDefault [] k mymap

isTuesday :: FilePath -> Bool
isTuesday str = let 
    dateStr = splitOn "." (last (splitOn "/" str)) !! 1
    year  = read (take 4 dateStr) :: Integer
    month = read (take 2 (drop 4 dateStr)) :: Int
    day   = read (take 2 (drop 6 dateStr)) :: Int
    in
        Tuesday == dayOfWeek (fromGregorian year month day)

takeSameDay :: [FilePath] -> [FilePath]
takeSameDay []          = []
takeSameDay (file:rest) = file:takeWhile isSameDay rest
    where
        isSameDay x = splitOn "." file !! 1 == splitOn "." x !! 1 

usage :: String
usage = "bgptometis month year while in the bgpdata file, currently does not recursively search for files."

main :: IO ()
main = do
    args   <- getArgs
    case args of
        (month:year:xs) -> do
            allFilePaths        <- getFiles month year
            let acceptedFiles = takeSameDay . filter isTuesday . filter isBZ2File $ allFilePaths
            putStrLn "BGP Files found:"
            for_ acceptedFiles $ \file -> do
                putStrLn file
            (rawASNMap, conMap) <- bimap unionsASNData unionsASConnections . unzip <$> forM acceptedFiles handleSingleFile
            putStrLn "Combining AS IP Ranges"
            pb1                 <- newProgressBar defStyle 10 (Progress 0 (length rawASNMap) ())
            asnMap              <- mapM (\x -> incProgress pb1 1 >> combineIPRanges x) rawASNMap
            let n = length . Map.keys  $ conMap
            putStrLn (show n ++ " nodes found")
            let m = length . concatMap Map.elems . Map.elems $ conMap
            putStrLn (show m ++ " edges found")
            let outputName = month ++ year
            let outMetis   = outputName ++ ".metis"
            let outNodemap = outputName ++ ".nodemap"
            let headerString = show n ++ ' ':show m ++ " 011"
            writeFile outMetis headerString
            writeFile outNodemap ""
            putStrLn "Formatting and writing output"
            pb2                  <- newProgressBar defStyle 10 (Progress 0 n ())
            for_ (Map.toAscList conMap) (\(asnNum, asnweights) -> do
                incProgress pb2 1
                appendFile outMetis ('\n':show (retrieveIPWeight asnNum asnMap)  ++ formatASConnections asnweights)
                appendFile outNodemap $ show asnNum ++ "\n")
        [] -> putStrLn usage
