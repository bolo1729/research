#!/usr/bin/python

# Copyright (c) 2013 Lukasz Bolikowski.
# All rights reserved.
# 
# Redistribution and use in source and binary forms are permitted
# provided that the above copyright notice and this paragraph are
# duplicated in all such forms and that any documentation,
# advertising materials, and other materials related to such
# distribution and use acknowledge that the software was developed
# by the <organization>.  The name of the
# <organization> may not be used to endorse or promote products derived
# from this software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.

from lxml import etree
import sqlite3
import sys
import time
import urllib

if len(sys.argv) < 3:
   print >> sys.stderr, 'Usage: %s <api_key> <output.db> [<offset>]' % sys.argv[0]
   sys.exit(1)

api_key = sys.argv[1]
db_filename = sys.argv[2]

offset = 0
if len(sys.argv) > 3:
   offset = int(sys.argv[3])

PAGE = 900
URL = 'http://api.plos.org/search?q=author_notes:*&fields=api_key=%s&fl=%s&start=%d&rows=%d'
FIELDS = ['id', 'article_type', 'journal', 'author', 'title', 'author_notes', 'publication_date', 'subject']

conn = sqlite3.connect(db_filename)

if offset == 0:
   conn.execute('CREATE TABLE document (id TEXT, title TEXT, publication_date TEXT, article_type TEXT, author_notes TEXT)')
   conn.execute('CREATE TABLE doc_author (id TEXT, author TEXT)')
   conn.execute('CREATE TABLE doc_subject (id TEXT, subject TEXT)')
   conn.commit()

total = sys.maxint
while offset < total:
   limit = min(PAGE, total - offset)
   query = URL % (api_key, ','.join(FIELDS), offset, limit)
   print >> sys.stderr, 'Executing query:', query
   handle = urllib.urlopen(query)
   # handle = open('test.xml')
   text = handle.read()
   handle.close()
   doc = etree.fromstring(text)

   total = int(doc.xpath('/response/result/@numFound')[0])

   items = doc.xpath('/response/result/doc')
   for item in items:
      try:
         doi = item.xpath('./str[@name="id"]')[0].text.strip()
         publication_date = item.xpath('./date[@name="publication_date"]')[0].text.strip()
         article_type = item.xpath('./str[@name="article_type"]')[0].text.strip()
         author_notes = item.xpath('./str[@name="author_notes"]')[0].text.strip()
         title = item.xpath('./str[@name="title"]')[0].text.strip()
         conn.execute('INSERT INTO document VALUES (?, ?, ?, ?, ?)', (doi, title, publication_date, article_type, author_notes))

         subjects = item.xpath('./arr[@name="subject"]/str')
         for subject in subjects:
            subject = subject.text.strip()
            conn.execute('INSERT INTO doc_subject VALUES (?, ?)', (doi, subject))

         authors = item.xpath('./arr[@name="author"]/str')
         for author in authors:
            author = author.text.strip()
            conn.execute('INSERT INTO doc_author VALUES (?, ?)', (doi, author))
      except IndexError:
         print >> sys.stderr, 'Skipping a record with incomplete information'

   conn.commit()
   offset += PAGE
   time.sleep(1)

conn.close()
