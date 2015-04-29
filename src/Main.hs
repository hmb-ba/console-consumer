module Main (
  main
)
where 

import Kafka.Client.Consumer 

import System.IO
import Network.Socket
import Data.IP
import Control.Concurrent ( threadDelay )
import Control.Monad
import qualified Data.ByteString.Char8 as C
import qualified Network.Socket.ByteString.Lazy as SBL
import qualified Data.ByteString as BS

import Control.Concurrent

main = do 
  -----------------
  -- Init Socket with user input
  -----------------
  sock <- socket AF_INET Stream defaultProtocol 
  setSocketOption sock ReuseAddr 1
  putStrLn "IP eingeben"
  ipInput <- getLine
  let ip = toHostAddress (read ipInput :: IPv4)
  putStrLn "Port eingeben"
  portInput <- getLine
  --let port = read portInput ::PortNumber  -- PortNumber does not derive from read
  connect sock (SockAddrInet 4343 ip)
  putStrLn "ClientId eingeben"
  clientId <- getLine
  putStrLn "TopicName eingeben"
  topicName <- getLine
  putStrLn "Ab Offset"
  fetchOffset <- getLine 

  forever $ do
    let req = packFtRqMessage $ InputFt
                                  (C.pack clientId) 
                                  (C.pack topicName)
                                  (fromIntegral $ (read fetchOffset ::Int))
    sendFtRequest sock req
    forkIO $ do
      input <- SBL.recv sock 4096
      let response = readFtResponse input
      print response
    threadDelay 1000000
  print "OK"

