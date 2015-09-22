import subprocess
from pipes import quote


class Pipeline(object):

    def __init__(self, initial=None):
        self.pipeline = []

        if initial is None:
            return

        for process in initial:
            self.add(process)

    def add(self, process):
        self.pipeline.append(process)

    def create_processes(self, stdin, stdout):
        processes = []
        total = len(self)

        for i in range(len(self)):
            process = self.pipeline[i]

            is_final = (i == total-1)
            is_first = (i == 0)

            proc_stdin = stdin if is_first else processes[i-1].stdout
            proc_stdout = stdout if is_final else subprocess.PIPE

            processes.append(process.run(
                stdin=proc_stdin,
                stdout=proc_stdout,
            ))

        return processes

    def run(self, stdin=None, stdout=None):
        processes = self.create_processes(stdin, stdout)
        last_process = processes[-1]
        first_process = processes[0]
        last_process.communicate()
        first_process.wait()
        return processes

    def __or__(self, process):
        if not isinstance(process, Process):
            raise TypeError('Unsupported operand type')
        self.add(process)
        return self

    def __len__(self):
        return len(self.pipeline)

    def __unicode__(self):
        return u' | '.join((unicode(p) for p in self.pipeline))

    __str__ = __unicode__

    def __repr__(self):
        return u'<{} {}>'.format(
            self.__class__.__name__,
            self.__unicode__(),
        )


class Process(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.popen_kwargs = kwargs

    def __or__(self, process):
        if not isinstance(process, Process):
            raise TypeError('Unsupported operand type')
        return Pipeline([self, process])

    def run(self, stdin=None, stdout=None, stderr=None):
        return subprocess.Popen(
            self.args,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            **self.popen_kwargs
        )

    def __unicode__(self):
        return quote(u' '.join(self.args))

    __str__ = __unicode__

    def __repr__(self):
        return u'<{} {}>'.format(
            self.__class__.__name__,
            self.__unicode__()
        )
