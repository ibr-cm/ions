import time

def startWatch():
    startTimeStampPerf = time.perf_counter()
    startTimeStampProcess = time.process_time()
    return (startTimeStampPerf, startTimeStampProcess)


def stopWatch(startTimeStamps):
    endTimeStampPerf = time.perf_counter()
    endTimeStampProcess = time.process_time()
    deltaTimePerf = endTimeStampPerf - startTimeStamps[0]
    deltaTimeProcess = endTimeStampProcess - startTimeStamps[1]
    return (deltaTimePerf, deltaTimeProcess)
