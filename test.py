import os
import sys
import mock
import shutil
import tempfile
import textwrap
import unittest2
from subprocess import PIPE

from vorlauf import Pipeline, Process


DEFAULT_WORDS_FILE = '/etc/dictionaries-common/words'


class PipelineConstructionTest(unittest2.TestCase):

    def setUp(self):
        self.single_process_pipeline = Pipeline([Process('foo', 'bar')])
        self.multiple_process_pipeline = Pipeline([
            Process('foo'),
            Process('bar baz'),
            Process('qux'),
        ])

    def test_pipeline_initial(self):
        pipeline = Pipeline()
        self.assertEqual(pipeline.pipeline, [])

    def test_pipeline_initial_list(self):
        process = Process('cat', 'foo')
        pipeline = Pipeline([process])
        self.assertEqual(pipeline.pipeline, [process])

    def test_pipeline_add(self):
        process = Process('cat', 'foo')
        pipeline = Pipeline()
        pipeline.add(process)
        self.assertEqual(pipeline.pipeline, [process])

    def test_pipeline_add_order(self):
        process1 = Process('first')
        process2 = Process('second')
        pipeline = Pipeline()
        pipeline.add(process1)
        pipeline.add(process2)
        self.assertEqual(pipeline.pipeline, [process1, process2])

    def test_pipeline_operator(self):
        process1 = Process('first')
        process2 = Process('second')
        process3 = Process('third')
        pipeline = process1 | process2 | process3
        self.assertEqual(pipeline.pipeline, [process1, process2, process3])

    def test_process_operator_wrong_operand(self):
        with self.assertRaises(TypeError):
            Process('first') | 'fail'

    def test_pipeline_operator_wrong_operand(self):
        with self.assertRaises(TypeError):
            Pipeline() | 'fail'

    def test_concatenate_pipelines(self):
        with self.assertRaises(TypeError):
            Process('first') | Pipeline()

    def test_pipeline_len(self):
        self.assertEqual(len(Pipeline()), 0)
        self.assertEqual(len(Pipeline([Process('foo')])), 1)

    def test_process_unicode(self):
        process = Process('foo', 'bar', 'baz')
        self.assertEqual(unicode(process), u"'foo bar baz'")

    def test_process_repr(self):
        process = Process('foo', 'bar', 'baz')
        self.assertEqual(repr(process), u"<Process 'foo bar baz'>")

    def test_pipeline_unicode(self):
        self.assertEqual(unicode(self.single_process_pipeline), u"'foo bar'")
        self.assertEqual(
            unicode(self.multiple_process_pipeline),
            u"foo | 'bar baz' | qux",
        )

    def test_pipeline_repr(self):
        self.assertEqual(
            repr(self.single_process_pipeline),
            u"<Pipeline 'foo bar'>"
        )
        self.assertEqual(
            repr(self.multiple_process_pipeline),
            u"<Pipeline foo | 'bar baz' | qux>",
        )


class ProcessRunTest(unittest2.TestCase):

    @mock.patch('vorlauf.subprocess')
    def test_cwd(self, mock_subprocess):
        process = Process('foo', cwd='/foo')
        process.run()
        mock_subprocess.Popen.assert_called_with(
            ('foo',), cwd='/foo', stderr=None, stdin=None, stdout=None
        )

    @mock.patch('vorlauf.subprocess')
    def test_env(self, mock_subprocess):
        env = {'foo': 'bar'}
        process = Process('foo', 'bar', env=env)
        process.run()
        mock_subprocess.Popen.assert_called_once_with(
            ('foo', 'bar'), env=env, stderr=None, stdin=None, stdout=None,
        )

    @mock.patch('vorlauf.subprocess')
    def test_duplicate_kwargs(self, mock_subprocess):
        process = Process('foo', 'bar', stdin='fail')
        with self.assertRaises(TypeError):
            process.run()

    @mock.patch('vorlauf.subprocess')
    def test_args(self, mock_subprocess):
        process = Process('foo', 'bar', 'baz')
        process.run()
        mock_subprocess.Popen.assert_called_once_with(
            ('foo', 'bar', 'baz'),
            stderr=None, stdin=None, stdout=None,
        )

    @mock.patch('vorlauf.subprocess')
    def test_returns_popen(self, mock_subprocess):
        mock_subprocess.Popen.return_value = 'MOCK_POPEN'
        process = Process('foo', 'bar')
        returned = process.run()
        self.assertEqual(returned, 'MOCK_POPEN')


class PipelineRunTest(unittest2.TestCase):

    def test_create_processes_single(self):
        mock_process = mock.Mock()
        pipeline = Pipeline([mock_process])
        processes = pipeline.create_processes('in', 'out')
        self.assertEqual(len(processes), 1)
        mock_process.run.assert_called_once_with(stdin='in', stdout='out')

    def test_create_processes_two(self):
        mock_process_1 = mock.Mock()
        mock_process_2 = mock.Mock()
        pipeline = Pipeline([mock_process_1, mock_process_2])
        processes = pipeline.create_processes('in', 'out')
        self.assertEqual(len(processes), 2)
        mock_process_1.run.assert_called_once_with(stdin='in', stdout=PIPE)
        mock_process_2.run.assert_called_once_with(
            stdin=mock_process_1.run().stdout, stdout='out'
        )

    def test_create_processes_three(self):
        mock_process_1 = mock.Mock()
        mock_process_2 = mock.Mock()
        mock_process_3 = mock.Mock()
        pipeline = Pipeline([mock_process_1, mock_process_2, mock_process_3])
        processes = pipeline.create_processes('in', 'out')
        self.assertEqual(len(processes), 3)
        mock_process_1.run.assert_called_once_with(stdin='in', stdout=PIPE)
        mock_process_2.run.assert_called_once_with(
            stdin=mock_process_1.run().stdout, stdout=PIPE
        )
        mock_process_3.run.assert_called_once_with(
            stdin=mock_process_2.run().stdout, stdout='out'
        )

    def test_run_single(self):
        mock_process = mock.Mock()
        pipeline = Pipeline([mock_process])
        processes = pipeline.run('in', 'out')
        self.assertEqual(len(processes), 1)
        mock_process.run.assert_called_once_with(stdin='in', stdout='out')
        # communicate and wait called on only process
        processes[0].communicate.assert_called_once_with()
        processes[0].wait.assert_called_once_with()

    def test_run_two(self):
        mock_process_1 = mock.Mock()
        mock_process_2 = mock.Mock()
        pipeline = Pipeline([mock_process_1, mock_process_2])
        processes = pipeline.run('in', 'out')
        self.assertEqual(len(processes), 2)
        # wait called on first
        processes[0].wait.assert_called_once_with()
        processes[0].communicate.assert_not_called()
        # communicate called on last
        processes[1].wait.assert_not_called()
        processes[1].communicate.assert_called_once_with()

    def test_run_three(self):
        mock_process_1 = mock.Mock()
        mock_process_2 = mock.Mock()
        mock_process_3 = mock.Mock()
        pipeline = Pipeline([mock_process_1, mock_process_2, mock_process_3])
        processes = pipeline.run('in', 'out')
        self.assertEqual(len(processes), 3)
        # wait called on first
        processes[0].wait.assert_called_once_with()
        processes[0].communicate.assert_not_called()
        # nothing called on second
        processes[1].wait.assert_not_called()
        processes[1].communicate.assert_not_called()
        # communicate called on last
        processes[2].wait.assert_not_called()
        processes[2].communicate.assert_called_once_with()


is_linux = sys.platform.startswith('linux')
words_file = os.environ.get('VORLAUF_WORDS_FILE', DEFAULT_WORDS_FILE)


def skip_unless_words(func):
    return unittest2.skipUnless(
        os.path.isfile(words_file),
        'Cannot find system words file {}'.format(words_file)
    )(func)


@unittest2.skipUnless(is_linux, 'integration tests currently only work on linux')
class LinuxIntegrationTest(unittest2.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix='vorlauf')
        self.stdout_path = os.path.join(self.directory, 'out')
        self.stdout = open(self.stdout_path, 'w+')
        self.fds = [self.stdout]

    def get_stdout(self):
        self.stdout.flush()
        self.stdout.seek(0)
        return self.stdout.read()

    def fd_from_string(self, string, name='in'):
        fd = open(os.path.join(self.directory, name), 'w+')
        self.fds.append(fd)
        fd.write(string)
        fd.seek(0)
        return fd

    def tearDown(self):
        for fd in self.fds:
            fd.close()

        shutil.rmtree(self.directory)

    def test_single_process(self):
        process = Process('md5sum')
        stdin = self.fd_from_string('fubar')
        popen_p = process.run(stdin=stdin, stdout=self.stdout)

        # pipelines do the waiting for us, Process object do not
        popen_p.wait()

        self.assertEqual(popen_p.returncode, 0)
        self.assertEqual(self.get_stdout(), '5185e8b8fd8a71fc80545e144f91faf2  -\n')

    def test_pipeline_single_process(self):
        process = Process('md5sum')
        pipeline = Pipeline([process])
        stdin = self.fd_from_string('fubar')
        popen_p, = pipeline.run(stdin=stdin, stdout=self.stdout)
        self.assertEqual(popen_p.returncode, 0)
        self.assertEqual(self.get_stdout(), '5185e8b8fd8a71fc80545e144f91faf2  -\n')

    def test_pipeline_multiple_process(self):
        pipeline = (
            Process('grep', 'fubar') |
            Process('uniq') |
            Process('head', '-c', '-1') |  # remove trailing newline :(
            Process('md5sum')
        )
        stdin = self.fd_from_string(textwrap.dedent('''
            foo
            bar
            bar
            baz
            fubar
            wiz
            bar
            qux
            fubar
        '''))
        pipeline.run(stdin=stdin, stdout=self.stdout)
        self.assertEqual(self.get_stdout(), '5185e8b8fd8a71fc80545e144f91faf2  -\n')

    def file_length(self, path):
        i = 0
        with open(path) as fd:
            for line in fd:
                i += 1
        return i

    @skip_unless_words
    def test_pipeline_single_process_large_stream(self):
        with open(words_file) as fd:
            pipeline = Pipeline()
            pipeline.add(Process('cat', words_file))
            pipeline.run(stdin=fd, stdout=self.stdout)

        self.assertEqual(
            self.file_length(words_file),
            self.file_length(self.stdout_path)
        )

    @skip_unless_words
    def test_pipeline_multiple_process_large_stream(self):
        pipeline = Process('cat', words_file) | Process('sort', '-r')
        pipeline.run(stdout=self.stdout)

        self.assertEqual(
            self.file_length(words_file),
            self.file_length(self.stdout_path),
        )


if __name__ == '__main__':
    unittest2.main()
