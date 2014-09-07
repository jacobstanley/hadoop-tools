{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -fdefer-type-errors #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_GHC -w #-}

module Main (main) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Exception (Exception, throwIO, handle)
import           Control.Monad
import           Control.Monad.IO.Class (MonadIO, liftIO)

import           Data.Bits ((.&.), shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as L
import           Data.Data (Data)
import           Data.IORef
import           Data.List (isPrefixOf)
import           Data.Maybe (fromMaybe, maybeToList)
import           Data.Monoid ((<>), mempty)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Time.Format (formatTime)
import           Data.Typeable (Typeable)
import           Data.Word (Word16, Word32, Word64)

import           System.Environment (getArgs)
import           System.FilePath
import           System.IO (Handle, BufferMode(..), hSetBuffering, hSetBinaryMode, hClose)
import           System.Locale (defaultTimeLocale)
import           Text.PrettyPrint.Boxes hiding ((<>))

import           Data.ProtocolBuffers
import           Data.ProtocolBuffers.Orphans ()
import           Data.Serialize.Get
import           Data.Serialize.Put

import           Data.Conduit
import           Data.Conduit.Cereal
import           Data.Conduit.Network

import           Hadoop.Protobuf.ClientNameNode
import           Hadoop.Protobuf.Hdfs
import           Hadoop.Protobuf.Headers

import           Options.Applicative hiding (Success)

------------------------------------------------------------------------

main :: IO ()
main = do
    cmd <- execParser optsParser
    handle printError (runTcp (app cmd))
  where
    optsParser = info (helper <*> options)
                      (fullDesc <> header "hh - Blazing fast interaction with HDFS")

    printError (RemoteError subject body) = T.putStrLn subject >> T.putStrLn body

------------------------------------------------------------------------

namenode :: ClientSettings
namenode = clientSettings 8020 "hadoop1"

username :: Text
username = "cloudera"

currentDir :: FilePath
currentDir = "/user/cloudera"

------------------------------------------------------------------------

app :: Command -> Conduit ByteString IO ByteString
app cmd = case cmd of
    List path      -> printListing path
    DiskUsage path -> printDiskUsage path

    Mkdir path parent -> do
      ok <- mkdirs path parent
      unless ok $ puts $ "Failed to create: " <> path

    Remove path recursive -> do
      ok <- delete path recursive
      unless ok $ puts $ "Failed to remove: " <> path
  where
    puts = liftIO . putStrLn

runTcp :: Remote a -> IO a
runTcp c = runTCPClient namenode $ \server -> do
    ref <- newIORef (error "_|_")
    let runWrite = login username >> c >>= liftIO . atomicWriteIORef ref
    appSource server =$= runWrite $$ appSink server
    readIORef ref

------------------------------------------------------------------------

type CreateParent = Bool
type Recursive = Bool

data Command = List FilePath
             | DiskUsage FilePath
             | Mkdir FilePath CreateParent
             | Remove FilePath Recursive

-- how the amount of space, in bytes, used by the files
options :: Parser Command
options = subparser $ command "ls"    (info ls    $ progDesc "List the contents of a directory")
                   <> command "du"    (info du    $ progDesc "Show the amount of space used by file or directory")
                   <> command "mkdir" (info mkdir $ progDesc "Create a directory in the specified location")
                   <> command "rm"    (info rm    $ progDesc "Delete a file or directory")
  where
    ls    = List      <$> argument str (dir       <> help "the directory to list")
    du    = DiskUsage <$> argument str (path      <> help "the file/directory to check the usage of")
    mkdir = Mkdir     <$> argument str (dir       <> help "the directory to create")
                      <*> switch       (short 'p' <> help "create intermediate directories")
    rm    = Remove    <$> argument str (path      <> help "the file/directory to remove")
                      <*> switch       (short 'r' <> help "recursively remove the whole file hierarchy")

    path = completer (fileCompletion (const True)) <> metavar "PATH"
    dir  = completer (fileCompletion (== Dir))     <> metavar "DIRECTORY"

fileCompletion :: (FileType -> Bool) -> Completer
fileCompletion p = mkCompleter $ \path -> handle ignore $ runTcp $ do
    let (dir, file) = splitFileName' path
    ls <- getListing dir

    return $ filter (path `isPrefixOf`)
           . map (getPath dir)
           . filter (p . get fsFileType)
           . concatMap (get dlPartialListing)
           . maybeToList  $ ls
  where
    ignore (RemoteError _ _) = return []

    splitFileName' p = case splitFileName p of
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

type Remote a = ConduitM ByteString ByteString IO a

data RemoteCall a = RemoteCall
    { rpcProtocolName    :: Text
    , rpcProtocolVersion :: Word64
    , rpcMethodName      :: Text
    , rpcBytes           :: ByteString
    , rpcDecode          :: ByteString -> Either RemoteError a
    }

data RemoteError = RemoteError Text Text
    deriving (Show, Eq, Data, Typeable)

instance Exception RemoteError

------------------------------------------------------------------------

get :: HasField a => (t -> a) -> t -> FieldType a
get f x = getField (f x)

------------------------------------------------------------------------

mkRemote :: (Decode b, Encode a) => Text -> Word64 -> Text -> a -> Remote b
mkRemote protocol ver method arg = invoke (RemoteCall protocol ver method (toBytes arg) fromBytes)

hdfs :: (Decode b, Encode a) => Text -> a -> Remote b
hdfs = mkRemote "org.apache.hadoop.hdfs.protocol.ClientProtocol" 1

getListing :: FilePath -> Remote (Maybe DirectoryListing)
getListing path = get lsDirList <$> hdfs "getListing" GetListingRequest
    { lsSrc          = putField (T.pack path')
    , lsStartAfter   = putField ""
    , lsNeedLocation = putField False
    }
  where
    -- TODO Move current directory to a config file
    path' = if "/" `isPrefixOf` path
            then path
            else currentDir </> path

getFileInfo :: FilePath -> Remote (Maybe FileStatus)
getFileInfo path = get fiFileStatus <$> hdfs "getFileInfo" GetFileInfoRequest
    { fiSrc = putField (T.pack path)
    }

getContentSummary :: FilePath -> Remote ContentSummary
getContentSummary path = get csSummary <$> hdfs "getContentSummary" GetContentSummaryRequest
    { csPath = putField (T.pack path)
    }

mkdirs :: FilePath -> CreateParent -> Remote Bool
mkdirs path createParent = get mdResult <$> hdfs "mkdirs" MkdirsRequest
    { mdSrc          = putField (T.pack path)
    , mdMasked       = putField (FilePermission (putField 0o755))
    , mdCreateParent = putField createParent
    }

delete :: FilePath -> Recursive -> Remote Bool
delete path recursive = get dlResult <$> hdfs "delete" DeleteRequest
    { dlSrc       = putField (T.pack path)
    , dlRecursive = putField recursive
    }

------------------------------------------------------------------------

login :: Text -> ConduitM ByteString ByteString IO ()
login user = sourcePut (putContext context)
  where
    context = IpcConnectionContext
        { ctxProtocol = putField (Just "hadoop-hs")
        , ctxUserInfo = putField (Just UserInformation
            { effectiveUser = putField (Just user)
            , realUser      = mempty
            })
        }

invoke :: RemoteCall a -> ConduitM ByteString ByteString IO a
invoke rpc = do
    sourcePut (putRequest header request)
    header <- sinkGet decodeLengthPrefixedMessage
    case get rspStatus header of
      Success -> sinkGet (rpcDecode rpc <$> getResponse) >>= throwLeft
      _       -> sinkGet getError >>= liftIO . throwIO
  where
    throwLeft (Left err) = liftIO (throwIO err)
    throwLeft (Right x)  = return x

    header = RpcRequestHeader
        { reqKind       = putField (Just ProtocolBuffer)
        , reqOp         = putField (Just FinalPacket)
        , reqCallId     = putField 1
        }

    request = RpcRequest
        { reqMethodName      = putField (rpcMethodName rpc)
        , reqBytes           = putField (Just (rpcBytes rpc))
        , reqProtocolName    = putField (rpcProtocolName rpc)
        , reqProtocolVersion = putField (rpcProtocolVersion rpc)
        }

------------------------------------------------------------------------

-- hadoop-2.1.0-beta is on version 9
-- see https://issues.apache.org/jira/browse/HADOOP-8990 for differences

putContext :: IpcConnectionContext -> Put
putContext ctx = do
    putByteString "hrpc"
    putWord8 7  -- version
    putWord8 80 -- auth method (80 = simple, 81 = kerberos/gssapi, 82 = token/digest-md5)
    putWord8 0  -- ipc serialization type (0 = protobuf)
    putBlob (toBytes ctx)

putRequest :: RpcRequestHeader -> RpcRequest -> Put
putRequest hdr req = putBlob (toLPBytes hdr <> toLPBytes req)

putBlob :: ByteString -> Put
putBlob bs = do
    putWord32be (fromIntegral (B.length bs))
    putByteString bs

getResponse :: Get ByteString
getResponse = do
    n <- fromIntegral <$> getWord32be
    getByteString n

getError :: Get RemoteError
getError = RemoteError <$> getText <*> getText
  where
    getText = do
        n <- fromIntegral <$> getWord32be
        T.decodeUtf8 <$> getByteString n

toBytes :: Encode a => a -> ByteString
toBytes = runPut . encodeMessage

toLPBytes :: Encode a => a -> ByteString
toLPBytes = runPut . encodeLengthPrefixedMessage

fromBytes :: Decode a => ByteString -> Either RemoteError a
fromBytes bs = case runGetState decodeMessage bs 0 of
    Left err      -> Left (RemoteError "fromBytes" (T.pack err))
    Right (x, "") -> Right x
    Right (_, _)  -> Left (RemoteError "fromBytes" "decoded response but did not consume enough bytes")

------------------------------------------------------------------------

type Path      = Text
type Owner     = Text
type Group     = Text
type Size      = Word64
type BlockRepl = Word32
type Perms     = Word16

formatFile :: Path -> Owner -> Group -> Size -> BlockRepl -> UTCTime -> FileType -> Perms -> Box
formatFile path o g sz mbr utc t p = text (formatMode t p)
                                 <+> text (if mbr == 0 then "-" else (show .fromIntegral) mbr)
                                 <+> text (T.unpack o)
                                 <+> text (T.unpack g)
                                 <+> text (show sz)
                                 <+> text (formatUTC utc)
                                 <+> text (T.unpack path)

formatSize :: Word64 -> String
formatSize b | b == 0               = "0"
             | b < 1000             = show b <> "B"
             | b < 1000000          = show (b `div` 1000) <> "K"
             | b < 1000000000       = show (b `div` 1000000) <> "M"
             | b < 1000000000000    = show (b `div` 1000000000) <> "G"
             | b < 1000000000000000 = show (b `div` 1000000000000) <> "T"

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

    conv bit str p | (p .&. bit) /= 0 = str
                   | otherwise        = "-"
