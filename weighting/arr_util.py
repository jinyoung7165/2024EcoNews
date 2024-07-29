import numpy as np
import pandas

class ArrUtil:
    def nparr_to_dataframe(self, arr, i, j):
        nparr = np.array(arr).reshape(i, j) # line수 * line수 배열로 만듦
        # 각 line별 유사도 합 구해서 배열에 넣기
        total_arr = nparr.sum(axis=1)
        nparr_total = np.array(total_arr).reshape(-1,1)
        result_arr = np.hstack((nparr, nparr_total)).reshape(i, j + 1)
        data_frame = pandas.DataFrame(result_arr, 
                                    index=[_ for _ in range(i)],
                                    columns = [_ for _ in range(j + 1)])
        return data_frame