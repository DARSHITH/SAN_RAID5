import pickle, logging
import argparse
import hashlib

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
RSM_UNLOCKED = bytearray(b'\x00') * 1
RSM_LOCKED = bytearray(b'\x01') * 1
cblk=-1
serverHits = 0


from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size):
    # This class stores the raw block array
    self.block = []
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      checkSum[i] = hashlib.md5(bytes(putdata)).hexdigest()
      #print(checkSum[i])
      self.block.insert(i,putdata)

if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()


  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-sid', '--sid', type=int, help='an integer value')
  ap.add_argument('-cblk', '--cblk', type=int, help='an integer value')


  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks') 
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.cblk:
    cblk=args.cblk

  checkSum = [0] * TOTAL_NUM_BLOCKS
  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE)
  #checkSum = ['f09f35a5637839458e462e6350ecbce4'] * TOTAL_NUM_BLOCKS
  #checkSum = []
  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)

  def Get(block_number):
    global serverHits
    serverHits += 1
    result = RawBlocks.block[block_number]
    result_hash = hashlib.md5(result).hexdigest()
    print("Number of server hits:" + str(serverHits))
    #print(result_hash,checkSum[block_number])
    if result_hash == checkSum[block_number]:
      return result
    else:
      #print("detected corrupt block number : "+str(block_number))
      return -1

  server.register_function(Get)

  def Put(block_number, data):
    global serverHits
    serverHits += 1
    RawBlocks.block[block_number] = bytearray(data.data)
    print("Number of server hits:" + str(serverHits))
    #print("block no: "+str(block_number))
    if args.cblk:
      blk_Num = cblk
      if block_number == blk_Num:
        temp = bytearray(bytes("data is corrupt", "utf-8"))
        data = bytes(bytearray(temp.ljust(BLOCK_SIZE,b'\x00')))
        checkSum[blk_Num] = hashlib.md5(data).hexdigest()
      else:
        checkSum[block_number] = hashlib.md5(data.data).hexdigest()
    else:
      checkSum[block_number] = hashlib.md5(data.data).hexdigest()
    return 0

  server.register_function(Put)

  def RSM(block_number):
    # global serverHits
    # serverHits += 1
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    #print("Number of server hits:" + str(serverHits))
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    return result

  server.register_function(RSM)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()

