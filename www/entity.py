import time
import uuid

from www.orm import Model, StringField, BooleanField, IntegerField, FloatField, TextField


def next_id():
    return '%015d%s000' % (int(time.time() * 1000), uuid.uuid4().hex)


class User(Model):
    __table__ = 'users'

    id = StringField(primary_key=True, default=next_id(), ddl='varchar(50)')
    email = StringField()
    passwd = StringField()
    admin = BooleanField()
    name = StringField()
    image = StringField(ddl='varchar(500')
    created_at = FloatField(default=time.time)


class Blog(Model):
    __table__ = 'blogs'

    id = StringField(primary_key=True, default=next_id(), ddl='varchar(50)')
    user_id = StringField()
    user_name = StringField()
    user_image = StringField(ddl='varchar(500')
    name = StringField()
    summary = StringField()
    content = TextField()
    create_at = FloatField(default=time.time)


class Comment(Model):
    __table__ = 'comments'

    id = StringField(primary_key=True,default=next_id(),ddl='varchar(50)')
    blog_id = StringField(ddl='varchar(50)')
    user_id = StringField(ddl='varchar(50)')
    user_name = StringField(ddl='varchar(50)')
    user_image = StringField(ddl='varchar(50)')
    content = TextField()
    created_at = FloatField(default=time.time)