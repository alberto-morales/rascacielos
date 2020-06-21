import sys, getopt
from scrapper import Scrapper

def main(argv):
   origin = None
   destination = None
   try:
      opts, args = getopt.getopt(argv,"ho:d:",["origin=","destination="])
   except getopt.GetoptError:
      print('launcher.py -o <origin> -d <destination>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('launcher.py -o <origin> -d <destination>')
         sys.exit()
      elif opt in ("-o", "--origin"):
         origin = arg
      elif opt in ("-d", "--destination"):
         destination = arg
   if (origin == None or destination == None):
       raise ValueError("origin & destination values required")
   #print(f'Origin is "{origin}"')
   #print(f'Destination is "{destination}"')
   scrapper = Scrapper({'origin': origin, 'destination': destination})
   scrapper.play()

if __name__ == "__main__":
   main(sys.argv[1:])