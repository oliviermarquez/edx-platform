"""
Test management command to cleanup resolved tasks.
"""

from datetime import timedelta

import ddt
from django.test import TestCase
from django.core.management import call_command
from django.utils.timezone import now

from openedx.core.djangolib.testing.utils import skip_unless_lms

from .... import models

DAY = timedelta(days=1)
MONTH_AGO = now() - (30 * DAY)


@ddt.ddt
@skip_unless_lms
class TestCleanupResolvedTasksCommand(TestCase):
    """
    Test cleanup_resolved_tasks management command.
    """

    def setUp(self):
        self.task_ids = [
            u'00000000-0000-0000-0000-000000000000',
            u'11111111-1111-1111-1111-111111111111',
            u'22222222-2222-2222-2222-222222222222',
            u'33333333-3333-3333-3333-333333333333',
        ]
        self.failed_tasks = [
            models.FailedTask.objects.create(
                task_name=u'task',
                datetime_resolved=MONTH_AGO - DAY,
                task_id=self.task_ids[0],
            ),
            models.FailedTask.objects.create(
                task_name=u'task',
                datetime_resolved=MONTH_AGO + DAY,
                task_id=self.task_ids[1],
            ),
            models.FailedTask.objects.create(
                task_name=u'task',
                datetime_resolved=None,
                task_id=self.task_ids[2],
            ),
            models.FailedTask.objects.create(
                task_name=u'other',
                datetime_resolved=MONTH_AGO - DAY,
                task_id=self.task_ids[3],
            ),
        ]
        super(TestCleanupResolvedTasksCommand, self).setUp()

    @ddt.data(
        ([], {2}),
        ([u'--task-name=task'], {2, 3}),
        ([u'--age=30'], {1, 2}),
        ([u'--age=30', u'--task-name=task'], {1, 2, 3}),
        #({}, {2}),
        #({u'task_name': u'task'}, {2, 3}),
        #({u'age': 30}, {1, 2}),
        #({u'age': 30, u'task_name': u'task'}, {1, 2, 3}),
    )
    @ddt.unpack
    def test_call_command(self, args, remaining_task_id_indices):
        call_command(u'cleanup_resolved_tasks', *args)
        results = set(models.FailedTask.objects.values_list('task_id', flat=True))
        remaining_task_ids = {self.task_ids[index] for index in remaining_task_id_indices}
        self.assertEqual(remaining_task_ids, results)
