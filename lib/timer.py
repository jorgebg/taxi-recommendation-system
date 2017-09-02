import time


class Timer:

    def __init__(self, jobid, description="", sc=None):
        self.jobid = jobid
        self.description = description
        if sc:
            sc.setJobGroup(jobid, description)

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        print("PFCDRIVER, %s, %s, %s" % (self.jobid, self.description, self.interval))
