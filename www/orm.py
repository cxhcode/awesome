import asyncio
import logging

import aiomysql


@asyncio.coroutine
def create_pool(loop, **kwargs):
    logging.info("create database connection pool")
    global __pool
    __pool = yield from aiomysql.create_pool(

        maxsize=kwargs.get('maxsize', 10),
        minsize=kwargs.get('minsize', 10),
        loop=loop,
        host=kwargs.get('host', '127.0.0.1'),
        port=kwargs.get('port', 3306),
        user=kwargs['user'],
        password=kwargs['password'],
        db=kwargs['db'],
        charset=kwargs.get('charset', 'utf8'),
        autocommit=kwargs.get('autocommit', True)
    )
    return __pool


@asyncio.coroutine
def select(sql, args, size=None):
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


@asyncio.coroutine
def execute(sql, args):
    global __pool__
    with (yield from __pool__) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args or ())
            afected = yield from cur.rowcount()
            yield from cur.close()
        except BaseException as e:
            raise e
        return afected


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 排除model类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join
        (escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join
        (map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['_delete__'] = 'delete from `%s` where `%s` = ?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        # super继承, 可以保证公共父类只被调用一次
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attr '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @asyncio.coroutine
    def save(cls):
        args = list(map(cls.getValueOrDefault,cls.__fields__))
        args.append(cls.getValueOrDefault(cls.__primary_key__))
        count = yield from execute(cls.__insert__, args)
        if count != 1:
            logging.warning('failed to insert')


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='int(11)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=None, ddl='tinyint'):
        super().__init__(name, ddl, False, default);


class TextField(Field):
    def __init__(self, name=None, default=None, ddl='text'):
        super().__init__(name, ddl, False, default);


class FloatField(Field):
    def __init__(self, name=None, default=None, ddl='float'):
        super().__init__(name, ddl, False, default);


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()


@asyncio.coroutine
def test(loop):
    yield from create_pool(loop, user='work', password='123456', db='awesome')
    yield from select('select * from users where id = ?', [1])
    rs = yield from User.find(1)
    print(rs)

# loop = asyncio.get_event_loop()
# loop.run_until_complete(test(loop))
# __pool.close()
# loop.run_until_complete(__pool.wait_closed())
# loop.close()
