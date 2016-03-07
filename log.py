import time


def log(text):
    def decorator(func):
        def wrapper(*args, **kw):
            print("call %s %s():" % (text, func.__name__))
            return func(*args, **kw)

        return wrapper

    return decorator


@log('execute')
def now():
    print("now is : %s" % time.time())


now()
print(now.__name__)
