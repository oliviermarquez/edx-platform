"""
Test management command to reapply failed tasks.
"""
from datetime import datetime

import celery
from django.test import TestCase
from django.core.management import call_command

from openedx.core.djangolib.testing.utils import skip_unless_lms

from .... import models, task


@skip_unless_lms
class TestReapplyTaskCommand(TestCase):
    """
    Test reapply_task management command.
    """

    task_name = u'openedx.core.djangoapps.celery_utils.management.commands.tests.test_reapply_tasks.example_task'
    other_task_name = u'openedx.core.djangoapps.celery_utils.management.commands.tests.test_reapply_tasks.other_task'

    @classmethod
    def setUpClass(cls):
        @celery.task(base=task.PersistOnFailureTask, name=cls.task_name)
        def example_task(error_message=None):
            """
            Simple task to let us test retry functionality.
            """
            if error_message:
                raise ValueError(error_message)

        cls.example_task = example_task

        @celery.task(base=task.PersistOnFailureTask, name=cls.other_task_name)
        def other_task():
            """
            This task always passes
            """
            return 5
        cls.other_task = other_task
        super(TestReapplyTaskCommand, cls).setUpClass()

    def setUp(self):
        self.task_ids = [
            u'00000000-0000-0000-0000-000000000000',
            u'11111111-1111-1111-1111-111111111111',
            u'22222222-2222-2222-2222-222222222222',
        ]
        self.failed_tasks = [
            models.FailedTask.objects.create(
                task_name=self.task_name,
                task_id=self.task_ids[0],
                args=[],
                kwargs={"error_message": "Err, yo!"},
                exc=u'UhOhError().',
            ),  # This task will fail again when run.
            models.FailedTask.objects.create(
                task_name=self.task_name,
                task_id=self.task_ids[1],
                args=[],
                kwargs={},
                exc=u'NetworkErrorMaybe?()',
            ),  # This task will complete successfully when run.
            models.FailedTask.objects.create(
                task_name=self.other_task_name,
                task_id=self.task_ids[2],
                args=[],
                kwargs={},
                exc=u'RaceCondition()',
            ),  # This task will complete successfully when run.
        ]
        super(TestReapplyTaskCommand, self).setUp()

    def test_call_command(self):
        call_command(u'reapply_tasks')
        self.assertIsNone(models.FailedTask.objects.get(task_id=self.task_ids[0]).datetime_resolved)
        self.assertIsInstance(models.FailedTask.objects.get(task_id=self.task_ids[1]).datetime_resolved, datetime)
        self.assertIsInstance(models.FailedTask.objects.get(task_id=self.task_ids[2]).datetime_resolved, datetime)

    def test_call_command_with_specified_task(self):
        call_command(u'reapply_tasks', u'--task-name={}'.format(self.task_name))
        self.assertIsNone(models.FailedTask.objects.get(task_id=self.task_ids[0]).datetime_resolved)
        self.assertIsInstance(models.FailedTask.objects.get(task_id=self.task_ids[1]).datetime_resolved, datetime)
        self.assertIsNone(models.FailedTask.objects.get(task_id=self.task_ids[2]).datetime_resolved)
