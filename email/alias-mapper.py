#!/usr/bin/env python

import sys, os, string
import shlex, subprocess

import redis

POSTFIX_CONFIG_FILE = "main.cf"
POSTFIX_ALIAS_FILE = "local.alias"

POSTFIX_MAP_CMD = "postalias -c %s hash:%s" 
POSTFIX_CONFIG_CMD = "postconf"

# This provides us with the default mappings
ALIASES="""
#
# Sample aliases file. Install in the location as specified by the
# output from the command "postconf alias_maps". Typical path names
# are /etc/aliases or /etc/mail/aliases.
#
#	>>>>>>>>>>      The program "newaliases" must be run after
#	>> NOTE >>      this file is updated for any changes to
#	>>>>>>>>>>      show through to Postfix.
#

# Person who should get root's mail. Don't receive mail as root!
#root:		you

# Basic system aliases -- these MUST be present
MAILER-DAEMON:	postmaster
postmaster:	root

# General redirections for pseudo accounts
bin:		root
daemon:		root
named:		root
nobody:		root
uucp:		root
www:		root
ftp-bugs:	root
postfix:	root

# Put your local aliases here.

# Well-known aliases
manager:	root
dumper:		root
operator:	root
abuse:		postmaster

# trap decode to catch security attacks
decode:		root

"""



class RedisSubscriber:
  def init_db( self ):
    self.redis_port = 6379
    self.redis_host = 'localhost'
    try:
      self.channel = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
      self.db = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
    except redis.exceptions.ConnectionError, e:
      print "check that your redis server is running on %s:%s" % (self.redis_host, self.redis_port)
      raise e

  def __init__( self, user, mapper ):
    self.user = user
    self.mapper = mapper

    self.init_db()

  def subscribe( self ):
    if not self.channel.subscribed:
      self.channel.subscribe("new-aliases:%s" % self.user)

  def unsubscribe( self ):
    if self.channel.subscribed:
      self.channel.unsubscribe("new-aliases:%s" % self.user)

  def run( self ):
    self.map()

    self.subscribe()

    go = True
    while (go):
      for msg in self.channel.listen():
        print msg
        if msg["data"] == "update":
          self.map()
        elif msg["data"] == "quit":
          go = False
          break

    self.unsubscribe()

  def map( self ):
    self.mapper.map(self.user, self.db.smembers("aliases:%s" % self.user))

class AliasMapper:
  def __init__( self, command=POSTFIX_MAP_CMD % (os.getcwd(), POSTFIX_ALIAS_FILE)):
    self.command = command

  def map( self, user, mapping ):
    alias = ALIASES
    for m in mapping:
      alias += "%s: \t %s \n" % (m, user)

    fp = open(os.path.join(os.getcwd(), POSTFIX_ALIAS_FILE), "w+")
    print >> fp, alias
    fp.close()

    print "running : ", self.command
    args = shlex.split(self.command)
    process = subprocess.Popen(args)
    process.wait()

class PostfixConfigGenerator:
  def __init__( self, command=POSTFIX_CONFIG_CMD ):
    self.command = command

  def generate( self ):
    args = shlex.split(self.command)
    process = subprocess.Popen(args, stdout=subprocess.PIPE)
    readlines = process.stdout.readlines()
    process.wait()
    config_array = []
    alias_database = alias_maps = 0
    for i, config_line in enumerate(readlines):
      k = config_line[:config_line.strip().index("=")].strip()
      v = config_line[config_line.strip().index("=")+1:].strip()
      if k == "alias_database":
        config_array.append("alias_database = hash:%s\n" % os.path.join(os.getcwd(), POSTFIX_ALIAS_FILE))
      elif k == "alias_maps":
        config_array.append("alias_maps = hash:%s\new" % os.path.join(os.getcwd(), POSTFIX_ALIAS_FILE))
      elif len(k) > 0 and len(v) > 0:
        config_array.append(config_line)

    config = string.join(config_array, "")
    fp = open(os.path.join(os.getcwd(), POSTFIX_CONFIG_FILE), "w+")
    print >> fp, config
    fp.close()

if __name__ == "__main__":
  import getpass

  subscriber = RedisSubscriber(getpass.getuser(), AliasMapper())

  if ( len(sys.argv) >= 2  and sys.argv[1] == "-c"):
    configer = PostfixConfigGenerator()
    configer.generate()

  subscriber.run()
