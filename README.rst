Vorlauf
=======

What?
-----

Vorlauf is a very minimal tool that helps you to create process pipelines (in
the shell sense). It also helps separate out the definition of a Process from
the running of a that process with a given stdin, stdout, and stderr.

Why?
----

Because the subprocess api for chaining processes together is cumbersome and
not very well documented.

How?
----

This library does basically nothing - it's implemented in fewer than 100 lines.
There are two classes available, ``Process`` and ``Pipeline``.

Process
~~~~~~~

The ``Process`` class is passed args, cwd, and env, and is executed by calling
``Process.run`` with optional ``stdin``, ``stdout``, and ``stderr`` parameters.

By removing the ``stdout``, ``stderr``, and ``stdin`` from the creation of the
``Process`` class, we can create reusable ``Process`` definitions::

  critical_grepper = Process('grep', 'CRITICAL')

  syslog = open('/var/log/syslog', 'r')
  apachelog = open('/var/log/httpd/error.log', 'r')

  filtered = open('critical.log', 'w')

  for logfile in (syslog, apachelog):
    critical_grepper.run(stdin=logfile, stdout=filtered)

Pipeline
~~~~~~~~

The ``Pipeline`` class stores a list of ``Process`` classes which, when run
with ``Pipeline.run`` with optional ``stdin`` and ``stdout``, pipes the
processes together. If present, ``stdin`` is passed to the first process, and
if present, ``stdout`` is passed to the last process.

Example
-------
::

  from vorlauf import Pipeline, Process

  pipeline = Pipeline()
  pipeline.add(Process('cat', 'foo.txt'))
  pipeline.add(Process('grep', 'something'))
  pipeline.add(Process('uniq'))

  with open('new.txt', 'wb') as fd:
      pipeline.run(stdout=fd)

And because of operator overloading built into the ``Process`` and ``Pipeline``
classes, this can be simplified as::

  from vorlauf import Process as P

  pipeline = P('cat', 'foo.txt') | P('grep', 'something') | P('uniq')
  with open('new.txt', 'wb') as fd:
      pipeline.run(stdout=fd)

Finally, you could use the ``Process`` class to create reusable components::

  from vorlauf import Process

  class GPG(Process):

      def __init__(self, passphrase):
          super(GPG, self).__init__('gpg', '-c', '--passphrase', passphrase, '-')


  class MySQLDump(Process):

      def __init__(self, password, dbname, **kwargs):
          super(MySQLDump, self).__init__(
              'mysqldump', '-u', 'root', '-p{}'.format(password), dbname
          )


  with open('mysql.dump', 'wb') as fd:
      pipeline = MySQLDump('loldongs', 'foo') | GPG('supersekrit')
      pipeline.run(stdout=fd)


Tests
-----

Run::

  virtualenv venv
  venv/bin/pip install -e .
  venv/bin/python test.py
