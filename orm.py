# coding: utf-8

# IMPORTANT!! remove ORM mapping redundancy
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import FLOAT, BIGINT
from sqlalchemy.types import Boolean, BLOB
from sqlalchemy import Column, Text, String, Integer, DateTime, ForeignKey, Unicode


Base = declarative_base()

class User(Base):

    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    username = Column(String(32), unique=True)

class Book(Base):

    __tablename__ = 'book'

    id = Column(Integer, primary_key=True)
    title = Column(String(500))
    description = Column(String(500))
    author_id = Column(ForeignKey('user.id'))
    for_user = Column(Text)
    learning_target = Column(Text)
    lang = Column(String(16))
    duration = Column(String(64))
    create_datetime = Column(DateTime)
    detail_content  = Column(Text)

    author = relationship('User')

class LibraryBook(Base):

    __tablename__ = 'library_book'

    id      = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey('book.id'))
    org_id = Column(Integer)
    category_id = Column(Integer)
    status = Column(String(1))
    borrow_limit = Column(Integer)
    borrow_duration = Column(Integer)
    uri_id = Column(String(16))
    update_datetime = Column(DateTime)
    recommendation = Column(Integer)

    book = relationship('Book')
    stats = relationship('LibraryBookStatistics')

class LibraryBookStatistics(Base):

    __tablename__ = 'library_book_statistics'

    id = Column(Integer, primary_key=True)
    lib_book_id = Column(Integer, ForeignKey('library_book.id'))

    pageview        = Column(BIGINT(display_width=32,unsigned=True))
    unique_pageview = Column(BIGINT(display_width=32,unsigned=True))
    app_pageview    = Column(BIGINT(display_width=32,unsigned=True))

    likes           = Column(BIGINT(display_width=32,unsigned=True))
    score           = Column(FLOAT)
    num_scores      = Column(BIGINT(display_width=32,unsigned=True))
    sum_scores      = Column(BIGINT(display_width=32,unsigned=True))

class Trec(Base):

    __tablename__ = 'trec'

    id = Column(Integer, primary_key=True)
    tcode = Column(String(64))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    lib_book_id = Column(Integer, ForeignKey('library_book.id'))
    owner_id = Column(Integer, ForeignKey('user.id'))
    title = Column(Text)
    num_tusers = Column(Integer)
    auth_key = Column(String(32))
    test_content = Column(BLOB)
    report_location = Column(Text)
    status = Column(String(1), default='A')

    lib_book = relationship('LibraryBook')

class TUser(Base):

    __tablename__ = 'tuser'

    id = Column(Integer, primary_key=True)
    join_dtime = Column(DateTime)
    user_nickname = Column(Unicode(64))
    identity = Column(String(32))
    trec_id  = Column(Integer, ForeignKey('trec.id'))
    auth_key = Column(String(32))
