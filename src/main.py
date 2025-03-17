from src.config import fetch_timeout_sec
from src.flow.TraceWeaver import TraceWeaver

if __name__ == '__main__':
    # 采用 serve 方式原地部署
    TraceWeaver.serve(name="TraceWeaver",
                      interval=fetch_timeout_sec
                      )
