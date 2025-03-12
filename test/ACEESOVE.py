###
import os 
import sys
from dotenv import load_dotenv

### take the environment variable from the .env file

load_dotenv()
print(os.getenv("MYUSER"))