import pandas as pd
import pyotp
from SmartApi import SmartConnect #or from SmartApi.smartConnect import SmartConnect
from logzero import logger
from angel_config.keys import config

api_key = config["key_id"]
username = config["username"]
pwd = config["pwd"]

smartApi = SmartConnect(api_key)
#print(smartApi)
try:
    token = config["totp"]
    totp = pyotp.TOTP(token).now()
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

correlation_id = "abcde"
data = smartApi.generateSession(username, pwd, totp)
#print(data)

historicParam={
        "exchange": "NSE",
        "symboltoken": "3045",
        "interval": "ONE_MINUTE",
        "fromdate": "2021-02-08 09:00",
        "todate": "2021-02-08 09:16"
        }
data = smartApi.getCandleData(historicParam)
data = pd.DataFrame(data["data"])
print(data)
'''try:
    logout=smartApi.terminateSession('Your Client Id')
    logger.info("Logout Successfull")
except Exception as e:
        logger.exception(f"Logout failed: {e}")'''