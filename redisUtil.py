from redis import StrictRedis

class RedisClient(StrictRedis):

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
