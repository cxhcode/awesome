import asyncio

import www.orm
from www.entity import User, Blog, Comment

#
# @asyncio.coroutine
# def testOrm(loop):
#     global __pool1
#     __pool1 = yield from www.orm.create_pool(loop, user='work', password='123456', db='awesome')
#     global u
#     u = User(name='Test', email='test@example.com')
#     yield from User.find(1)
#
#
# loop1 = asyncio.get_event_loop()
# loop1.run_until_complete(testOrm(loop1))
#
# __pool1.close()
# loop1.run_until_complete(__pool1.wait_closed())
# loop1.close()



@asyncio.coroutine
def test(loop):
    global pool
    pool = yield from www.orm.create_pool(loop, user='work', password='123456', db='awesome')
    print(pool)
    rs = yield from User.find(1)
    print(rs)
    yield from User(email='jack@em.com').save()


loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
pool.close()
loop.run_until_complete(pool.wait_closed())
loop.close()
