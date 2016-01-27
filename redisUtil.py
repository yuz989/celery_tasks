from redis import StrictRedis

class RedisClient(StrictRedis):

    def zrem_bulk(self, key, right, left=0, withscores=True):
        pipe = self.pipeline() # transaction = True

        pairs = self.zrange(key, left , right, withscores=withscores)
        self.zremrangebyrank(key, left , right)
        pipe.execute()

        return pairs

    def sadd_bulk(self, key, list):
        pipe = self.pipeline()
        for item in list:
            self.sadd(key, item)
        pipe.execute()

    def spop_bulk(self, key, count):
        pipe = self.pipeline()
        pops = []
        for i in range(0,count):
            temp = self.spop(key)
            if not temp:
                break
            else:
                pops.append(temp)
        pipe.execute()
        return pops
