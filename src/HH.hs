{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Exception (handle)
import           Control.Monad
import           Control.Monad.IO.Class (liftIO)

import           Data.Bits ((.&.), shiftR)
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as H
import           Data.List (intercalate)
import           Data.List (isPrefixOf)
import           Data.List.Split (splitOn)
import           Data.Maybe (fromMaybe, maybeToList)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import qualified Data.Text.Read as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word (Word16, Word32, Word64)

import           Data.Ini
import           System.Environment (getEnv)
import           System.FilePath
import           System.IO.Unsafe (unsafePerformIO)
import           System.Locale (defaultTimeLocale)
import           Text.PrettyPrint.Boxes hiding ((<>))

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()

import           Hadoop.Protobuf.Hdfs
import           Hadoop.Rpc

import           Options.Applicative hiding (Success)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    handle printError (runRemote (app cmd))
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

    printError (RemoteError subject body) = T.putStrLn subject >> T.putStrLn body

runRemote :: Remote a -> IO a
runRemote remote = case socksProxy of
    Nothing -> runTcp remote' nameNode
    Just sp -> runSocks remote' sp nameNode
  where
    remote' = login username >> remote

------------------------------------------------------------------------

-- TODO: Fix this unsafe mess :) it's not that unsafe but it's also not nice.

fromEither :: String -> Either String a -> a
fromEither prefix (Left err) = error (prefix <> ": " <> err)
fromEither _      (Right x)  = x

configPath :: FilePath
configPath = unsafePerformIO $ do
    home <- getEnv "HOME"
    return (home </> ".hh")
{-# NOINLINE configPath #-}

config :: Ini
config = unsafePerformIO $ fromEither configPath <$> readIniFile configPath
{-# NOINLINE config #-}

socksProxy :: Maybe SocksProxy
socksProxy = either (const Nothing) Just $ do
    host <- lookupValue "socks-proxy" "host" config
    port <- readValue "socks-proxy" "port" T.decimal config
    return (Endpoint host port)

nameNode :: NameNode
nameNode = fromEither configPath $ do
    host <- lookupValue "namenode" "host" config
    port <- readValue "namenode" "port" T.decimal config
    return (Endpoint host port)

username :: Text
username = fromEither configPath (lookupValue "hdfs" "username" config)

currentDir :: FilePath
currentDir = fromEither configPath (T.unpack <$> lookupValue "hdfs" "current-dir" config)

setCurrentDir :: FilePath -> IO ()
setCurrentDir path = do
    (Ini ini) <- fromEither configPath <$> readIniFile configPath
    let absPath = T.pack (getAbsolute path)
        ini' = H.adjust (H.insert "current-dir" absPath) "hdfs" ini
    writeIniFile configPath (Ini ini')

getAbsolute :: FilePath -> FilePath
getAbsolute path | isPrefixOf "/" path = normalizePath path
                 | otherwise           = normalizePath (currentDir </> path)

normalizePath :: FilePath -> FilePath
normalizePath = intercalate "/" . dropAbsParentDir . splitOn "/"

dropAbsParentDir :: [FilePath] -> [FilePath]
dropAbsParentDir []       = error "dropAbsParentDir: not an absolute path"
dropAbsParentDir (p : ps) = p : (reverse $ fst $ go [] ps)
  where
    go []       (".." : ys) = go [] ys
    go (_ : xs) (".." : ys) = go xs ys
    go xs       (y    : ys) = go (y : xs) ys
    go xs       []          = (xs, [])

------------------------------------------------------------------------

app :: Command -> Remote ()
app cmd = case cmd of
    List path      -> printListing $ getAbsolute $ maybe "" id path
    DiskUsage path -> printDiskUsage $ getAbsolute $ maybe "" id path

    Chdir path -> liftIO (setCurrentDir path)

    Mkdir path parent -> do
      let absPath = getAbsolute path
      ok <- mkdirs absPath parent
      unless ok $ puts $ "Failed to create: " <> absPath

    Remove path recursive -> do
      let absPath = getAbsolute path
      ok <- delete absPath recursive
      unless ok $ puts $ "Failed to remove: " <> absPath
  where
    puts = liftIO . putStrLn

------------------------------------------------------------------------

data Command = List (Maybe FilePath)
             | DiskUsage (Maybe FilePath)
             | Chdir FilePath
             | Mkdir FilePath CreateParent
             | Remove FilePath Recursive

-- how the amount of space, in bytes, used by the files
options :: Parser Command
options = subparser $ command "ls"    (info ls    $ progDesc "List the contents of a directory")
                   <> command "du"    (info du    $ progDesc "Show the amount of space used by file or directory")
                   <> command "cd"    (info chdir $ progDesc "Change the current directory")
                   <> command "mkdir" (info mkdir $ progDesc "Create a directory in the specified location")
                   <> command "rm"    (info rm    $ progDesc "Delete a file or directory")
  where
    ls = List      <$> optional (argument str (dir  <> help "the directory to list"))
    du = DiskUsage <$> optional (argument str (path <> help "the file/directory to check the usage of"))

    chdir = Chdir     <$> argument str (dir       <> help "the directory to change to")
    mkdir = Mkdir     <$> argument str (dir       <> help "the directory to create")
                      <*> switch       (short 'p' <> help "create intermediate directories")
    rm    = Remove    <$> argument str (path      <> help "the file/directory to remove")
                      <*> switch       (short 'r' <> help "recursively remove the whole file hierarchy")

    path = completer (fileCompletion (const True)) <> metavar "PATH"
    dir  = completer (fileCompletion (== Dir))     <> metavar "DIRECTORY"

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \path -> handle ignore $ runRemote $ do
    let (dir, _) = splitFileName' path
    ls <- getListing (getAbsolute dir)

    return $ filter (path `isPrefixOf`)
           . map (getPath dir)
           . filter (p . get fsFileType)
           . concatMap (get dlPartialListing)
           . maybeToList  $ ls
  where
    ignore (RemoteError _ _) = return []

    splitFileName' x = case splitFileName x of
        ("./", f) -> ("", f)
        (d, f)    -> (d, f)

getPath :: FilePath -> FileStatus -> FilePath
getPath parent file = parent </> B.unpack (get fsPath file) <> suffix
  where
    suffix = case get fsFileType file of
        Dir -> "/"
        _   -> ""

------------------------------------------------------------------------

printDiskUsage :: FilePath -> Remote ()
printDiskUsage path = do
    mls <- getListing path
    case mls of
      Nothing -> liftIO $ putStrLn $ "File/directory does not exist: " <> path
      Just ls -> do
        let files = map (getPath path) (get dlPartialListing ls)
        css <- zip files <$> mapM getContentSummary files

        let col a f = vcat a (map (text . f) css)

        liftIO $ do
            printBox $ col right (formatSize . get csLength . snd)
                   <+> col left  fst

printListing :: FilePath -> Remote ()
printListing path = do
    mls <- getListing path
    case mls of
      Nothing -> liftIO . putStrLn $ "Directory does not exist: " <> path
      Just ls -> do
        let getPerms     = fromIntegral . get fpPerm . get fsPermission
            getBlockRepl = fromMaybe 0 . get fsBlockReplication

            hdfs2utc ms  = posixSecondsToUTCTime (fromIntegral ms / 1000)
            getModTime   = hdfs2utc . get fsModificationTime

            -- TODO: Fetch rest of partial listing
            xs      = get dlPartialListing ls
            col a f = vcat a (map (text . f) xs)

        liftIO $ do
            putStrLn $ "Found " <> show (length xs) <> " items"

            printBox $ col left  (\x -> formatMode (get fsFileType x) (getPerms x))
                   <+> col right (formatBlockRepl . getBlockRepl)
                   <+> col left  (T.unpack . get fsOwner)
                   <+> col left  (T.unpack . get fsGroup)
                   <+> col right (formatSize . get fsLength)
                   <+> col right (formatUTC . getModTime)
                   <+> col left  (T.unpack . T.decodeUtf8 . get fsPath)

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)

------------------------------------------------------------------------

type Perms = Word16

formatSize :: Word64 -> String
formatSize b | b <= 0            = "0"
             | b < 1000          = show b <> "B"
             | b < 1000000       = show (b `div` 1000) <> "K"
             | b < 1000000000    = show (b `div` 1000000) <> "M"
             | b < 1000000000000 = show (b `div` 1000000000) <> "G"
             | otherwise         = show (b `div` 1000000000000) <> "T"

formatBlockRepl :: Word32 -> String
formatBlockRepl x | x == 0    = "-"
                  | otherwise = show x

formatUTC :: UTCTime -> String
formatUTC = formatTime defaultTimeLocale "%Y-%m-%d %H:%M"

formatMode :: FileType -> Perms -> String
formatMode File    = ("-" <>) . formatPerms
formatMode Dir     = ("d" <>) . formatPerms
formatMode SymLink = ("l" <>) . formatPerms

formatPerms :: Perms -> String
formatPerms perms = format (perms `shiftR` 6)
                 <> format (perms `shiftR` 3)
                 <> format perms
  where
    format p = conv 0x4 "r" p
            <> conv 0x2 "w" p
            <> conv 0x1 "x" p

    conv bit rep p | (p .&. bit) /= 0 = rep
                   | otherwise        = "-"
