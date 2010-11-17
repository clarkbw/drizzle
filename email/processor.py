import sys, os, time, re

import redis

from email import message_from_file
from email.utils import unquote, getaddresses
import datetime
from dateutil.tz import tzutc, tzlocal
from email.utils import mktime_tz, parsedate_tz
from email.header import decode_header
from email.Iterators import typed_subpart_iterator

import time
#
#   How often to check for new images to upload  (in seconds )
#
SLEEP_TIME = 1 * 20

class ConversationProcessor:
  db = None
  user = None

  def __init__( self, user, db ):
    self.user = user
    self.db = db

  def process( self, msg_id ):

    conv_id = None
    known_conversations = []
    unknown_references = []

    references = self.db.lrange("message:%s:header:%s" % (msg_id, "references"), 0, -1)
    for reference in references:
      conv_id = self.db.get("message:%s:conversation-id" % (msg_id))
      if conv_id: conversation.append(conv_id)
      else: unknown_references.append(msg_id)

    if len(known_conversations) > 1:
      conv_id = known_conversations.pop()
      for id in known_conversations:
        # merge all references stored in this conversation
        messages = self.db.smembers("conversation:%s:messages" % id)
        self.db.sunionstore("conversation:%s:messages" % conv_id, "conversation:%s:messages" % id)
        self.db.delete("conversation:%s:messages" % id)

        # overwrite all the msg header id to conversation id mappings
        for _msg_id in messages:
          _msg_header_id = self.db.lrange("message:%s:header:%s" % (_msg_id, "message-id"), 0, -1)[0]
          self.db.set("message:%s:conversation-by-header-id" % (_msg_header_id), conv_id)

        # merge all the msg involves sets
        self.db.sunionstore("conversation:%s:involves" % conv_id, "conversation:%s:involves" % (id))
        self.db.delete("conversation:%s:involves" % id)

        # merge the timestamps to the latest time
        other_conv_utctimestamp = int(self.db.get("conversation:%s:utctimestamp" % (id)))
        conv_utctimestamp = int(self.db.get("conversation:%s:utctimestamp" % (conv_id)))
        if other_conv_utctimestamp > conv_utctimestamp:
          self.db.set("conversation:%s:utctimestamp" % (conv_id), other_conv_utctimestamp)
          self.db.set("conversation:%s:timestamp" % (conv_id), self.db.get("conversation:%s:timestamp" % (id)))
        self.db.delete("conversation:%s:utctimestamp" % (id))
        self.db.delete("conversation:%s:timestamp" % (id))

        # merge all the specific involvement sets
        for involvement in ["to","cc", "bcc", "from"]:
          self.db.sunionstore("conversation:%s:%s" % (conv_id, involvement), "conversation:%s:%s" % (id, involvement))
          self.db.delete("conversation:%s:%s" % (id, involvement))

        # delete the conversation from our master list
        self.db.zrem("all-conversations:%s" % (self.user), id)

    elif len(known_conversations) == 1:
      # we have a single conversation, things are good
      conv_id = known_conversations.pop()
    else:
      # we need to create a new conversation object
      conv_id = self.db.incr("ids:%s:conversation" % self.user)
      utctimestamp = self.db.get("message:%s:utctimestamp" % (msg_id))
      self.db.zadd("all-conversations:%s" % (self.user), conv_id, utctimestamp)
      subject = self.db.lrange("message:%s:header:%s" % (msg_id, "subject"), 0, -1)[0]
      self.db.set("conversation:%s:subject" % (conv_id), subject)
      self.db.set("conversation:%s:utctimestamp" % (conv_id), utctimestamp)
      self.db.set("conversation:%s:timestamp" % (conv_id), self.db.get("message:%s:timestamp" % (msg_id)))

    # Run these merges specifically against our message in question
    msg_header_id = self.db.lrange("message:%s:header:%s" % (msg_id, "message-id"), 0, -1)[0]
    self.db.set("message:%s:conversation-by-header-id" % (msg_header_id), conv_id)

    for involvement in ["to","cc", "bcc", "from"]:
      self.db.sunionstore("conversation:%s:%s" % (conv_id, involvement), ["conversation:%s:%s" % (conv_id, involvement), "message:%s:%s" % (msg_id, involvement)])
      self.db.sunionstore("conversation:%s:involves" % conv_id, ["conversation:%s:involves" % conv_id, "message:%s:%s" % (msg_id, involvement)])

    msg_utctimestamp = int(self.db.get("message:%s:utctimestamp" % (msg_id)))
    conv_utctimestamp = int(self.db.get("conversation:%s:utctimestamp" % (conv_id)))
    if msg_utctimestamp > conv_utctimestamp:
      self.db.set("conversation:%s:utctimestamp" % (conv_id), msg_utctimestamp)
      self.db.set("conversation:%s:timestamp" % (conv_id), self.db.get("message:%s:timestamp" % (msg_id)))
      self.db.zadd("all-conversations:%s" % (self.user), conv_id, msg_utctimestamp)

    # Set all the unknown references to point to our conversation
    for unknown_reference_id in unknown_references:
      self.db.set("message:%s:conversation-by-header-id" % (unknown_reference_id), conv_id)

    #headers = self.db.lrange("message:%s:headers" % msg_id, 0, -1)

class EmailProcessor:
  db = None
  user = None

  def __init__( self, user, db ):
    self.user = user
    self.db = db

  def process( self, msg ):
    msg_id = self.db.incr("ids:%s:message" % self.user)

    self.process_body(msg_id, msg)

    self.process_headers(msg_id, msg)

    self.db.sadd("all-messages:%s" % (self.user), msg_id)

    return msg_id

  def process_body( self, msg_id, msg ):
    email_body = ""
    def get_charset( msg, default="ascii" ):
      """Get the message charset"""
      if msg.get_content_charset(): return msg.get_content_charset();
      if msg.get_charset(): return msg.get_charset();
      return default

    if msg.is_multipart():
      parts = [part for part in typed_subpart_iterator(msg,'text','plain')]
      body = []
      for part in parts:
        charset = get_charset(part, get_charset(msg))
        body.append(unicode(part.get_payload(decode=True), charset, "replace"))

      email_body = u"\n".join(body).strip()

    else: # if it is not multipart, the payload will be a string
        # representing the message body
      body = unicode(msg.get_payload(decode=True),
                     get_charset(msg),
                     "replace")
      email_body = body.strip()

    self.db.set("message:%s:body" % msg_id, email_body)

  def process_headers( self, msg_id, msg ):
    # Given we have no opportunity to introduce an object which can ignore
    # the case of headers, we lowercase the keys
    headers = {}
    for hn in msg.keys():
      header_values = msg.get_all(hn)
      if header_values:
        header_name = hn.lower()
        # add this header to the list of available headers
        self.db.rpush("message:%s:headers" % (msg_id), header_name)

        # do any charset etc conversion on the values...
        header_values = [self._safe_convert_header(v) for v in header_values]

        # go through the values converting them into usable lists
        for value in header_values:
          if re.match(r"<.+>,",value):
            for v in value.split(","):
              self.db.rpush("message:%s:header:%s" % (msg_id, header_name), unquote(v.strip()))
          # multiple reference processing
          elif header_name == "references" and re.match(r"<[^<>]+>\s+",value):
            for ref in re.findall(r"<[^<>]+>",value):
              self.db.rpush("message:%s:header:%s" % (msg_id, header_name), unquote(ref.strip()))
          else:
            self.db.rpush("message:%s:header:%s" % (msg_id, header_name), unquote(value.strip()))

        if header_name in ["to","cc", "bcc", "from"]:
          for name, address in getaddresses(header_values):
            contact_id = None

            if self.db.sadd("all-addresses:%s" % self.user, address) == 1:
              # create a new contact id
              contact_id = self.db.incr("ids:%s:contact" % self.user)

              # a new contact, add them to our new-contacts list for later processing
              self.db.sadd("new-contacts:%s" % self.user, contact_id)

              # set an id lookup for the email address
              self.db.set("contact:%s:%s" % (self.user, address), contact_id)

              # set the contact hash data
              self.db.hset("contact:%s:%s" % (self.user, contact_id), "name", name)
              self.db.sadd("contact:%s:%d:emails" % (self.user, contact_id), address)
            else:
              contact_id = self.db.get("contact:%s:%s" % (self.user, address))

            self.db.sadd("message:%s:%s" % (msg_id, header_name), contact_id)

        elif header_name in ["date"]:
          utctimestamp = int(mktime_tz(parsedate_tz(value)))
          timestamp = datetime.datetime.fromtimestamp(utctimestamp, tzutc())
          self.db.set("message:%s:utctimestamp" % (msg_id), utctimestamp)
          self.db.set("message:%s:timestamp" % (msg_id), timestamp)


  def _safe_convert_header( self, header_val, default="ascii" ):
    headers = decode_header(header_val)
    header_sections = [unicode(text, charset or default) for text, charset in headers]
    return u"".join(header_sections)

class EmailMonitor:
  last_checked = None
  directory = "."
  user = None
  processed = None
  email_processor = None
  conversation_processor = None
  db = None

  def update_last_checked( self ):
    last_checked = time.localtime()

  def init_db( self ):
    self.redis_port = 6379
    self.redis_host = 'localhost'
    try:
      self.db = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
      self.db.setnx("ids:%s:message" % self.user, 1)
      self.db.setnx("ids:%s:conversation" % self.user, 1)
      self.db.setnx("ids:%s:contact" % self.user, 1)
    except redis.exceptions.ConnectionError, e:
      print "check that your redis server is running on %s:%s" % (self.redis_host, self.redis_port)
      raise e

  def __init__( self, user, dir ):
    self.user = user
    self.directory = os.path.expanduser(dir)
    self.init_db()
    self.email_processor = EmailProcessor(self.user, self.db)
    self.conversation_processor = ConversationProcessor(self.user, self.db)

  def check( self ):
    emails = self.get_new_emails()
    for e in emails:
      msg = message_from_file(open(os.path.join(e.get("path"), e.get("file"))))
      msg_id = self.email_processor.process(msg)
      self.db.sadd("files:%s:%s" % (self.user,e.get("path")), e.get("file"))
      self.conversation_processor.process(msg_id)

  def get_new_emails( self ):
    emails = []
    walk = os.walk( self.directory )

    for data in walk:
        (path, directories, filenames) = data
        processed_files = self.db.smembers("files:%s:%s" % (self.user,path)) or []
        print path, processed_files
        for f in filenames :
            if not (f in processed_files):
              emails.append({ "file" : f, "path" : os.path.normpath(path)})

    self.update_last_checked()

    return emails

  def run( self ):
    while ( True ):
      self.check()
      print "Last check: " , str( time.asctime(self.last_checked) )
      time.sleep( SLEEP_TIME )

if __name__ == "__main__":
  import getpass
  monitor = EmailMonitor(getpass.getuser(), "~/Maildir")
  
  if ( len(sys.argv) >= 2  and sys.argv[1] == "-d"):
    monitor.run()
  else:
    monitor.check()
