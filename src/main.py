from src.config import fetch_timeout_sec
from src.flow.SeeFlow import SeeFlow

if __name__ == '__main__':
    # 采用 serve 方式原地部署
    SeeFlow.serve(name="SeeFlow",
                  interval=fetch_timeout_sec
                  )
