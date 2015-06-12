module Main (
  main
)
where 

import Control.Concurrent
import Control.Monad

import Data.IP
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString as BS

import Kafka.Client

import Network.Socket
import qualified Network.Socket.ByteString.Lazy as SBL

import System.IO

main = do
  -----------------
  -- Init Socket with user input
  -----------------
  sock <- socket AF_INET Stream defaultProtocol 
  setSocketOption sock ReuseAddr 1
  putStrLn "Give IP"
  ipInput <- getLine
  let ip = toHostAddress (read ipInput :: IPv4)

  putStrLn "Give Port"
  portInput <- getLine
  connect sock (SockAddrInet 4343 ip)
  putStrLn "ClientId eingeben"
  client <- getLine

  let requestHeader = Head 0 0 (stringToClientId client)

  -------------------
  -- Get Metadata from broker
  ------------------
  let mdReq = Metadata requestHeader []
  sendRequest sock mdReq
  mdInput <- SBL.recv sock 4096
  let mdRes = decodeMdResponse mdInput
  print "Brokers Metadata:"
  print  mdRes

  ---------------
  -- Start Consuming
  --------------
  putStrLn "Give Topic Name"
  topicName <- getLine
  let t = stringToTopic topicName

  putStrLn "Give Partition Number"
  partition <- getLine
  let p = read partition :: Int

  putStrLn "Give Offset"
  fetchOffset <- getLine
  let o = read fetchOffset :: Int 

  let ftReq = Fetch requestHeader [ FromTopic t [ FromPart p o ]]
  forever $ do

    sendRequest sock $ ftReq
    forkIO $ do
      input <- SBL.recv sock 4096
      print input
      let response = decodeFtResponse input
      print response
    threadDelay 1000000
  print "OK"

