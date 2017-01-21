"""
Command to load course blocks.
"""
from collections import defaultdict
import logging

from django.core.management.base import BaseCommand
from xmodule.modulestore.django import modulestore

import openedx.core.djangoapps.content.block_structure.api as api
import openedx.core.djangoapps.content.block_structure.tasks as tasks
import openedx.core.lib.block_structure.cache as cache
from openedx.core.lib.command_utils import (
    get_mutually_exclusive_required_option,
    validate_mutually_exclusive_option,
    validate_dependent_option,
    parse_course_keys,
)


log = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Example usage:
        $ ./manage.py lms generate_course_blocks --all --settings=devstack
        $ ./manage.py lms generate_course_blocks 'edX/DemoX/Demo_Course' --settings=devstack
    """
    args = '<course_id course_id ...>'
    help = 'Generates and stores course blocks for one or more courses.'

    def add_arguments(self, parser):
        """
        Entry point for subclassed commands to add custom arguments.
        """
        parser.add_argument(
            '--courses',
            dest='courses',
            nargs='+',
            help='Generate course blocks for the list of courses provided.',
        )
        parser.add_argument(
            '--all_courses',
            help='Generate course blocks for all courses, given the requested start and end indices.',
            action='store_true',
            default=False,
        )
        parser.add_argument(
            '--enqueue_task',
            help='Enqueue the tasks for asynchronous computation.',
            action='store_true',
            default=False,
        )
        parser.add_argument(
            '--routing_key',
            dest='routing_key',
            help='Routing key to use for asynchronous computation.',
        )
        parser.add_argument(
            '--force_update',
            help='Force update of the course blocks for the requested courses.',
            action='store_true',
            default=False,
        )
        parser.add_argument(
            '--start_index',
            help='Starting index of course list.',
            default=0,
            type=int,
        )
        parser.add_argument(
            '--end_index',
            help='Ending index of course list.',
            default=0,
            type=int,
        )
        parser.add_argument(
            '--dags',
            help='Find and log DAGs for all or specified courses.',
            action='store_true',
            default=False,
        )

    def handle(self, *args, **options):

        courses_mode = get_mutually_exclusive_required_option(options, 'courses', 'all_courses')
        validate_mutually_exclusive_option(options, 'enqueue_task', 'dags')
        validate_dependent_option(options, 'routing_key', 'enqueue_task')
        validate_dependent_option(options, 'start_index', 'all_courses')
        validate_dependent_option(options, 'end_index', 'all_courses')

        if courses_mode == 'all_courses':
            course_keys = [course.id for course in modulestore().get_course_summaries()]
            if options.get('start_index'):
                end = options.get('end_index') or len(course_keys)
                course_keys = course_keys[options['start_index']:end]
        else:
            course_keys = parse_course_keys(options['courses'])

        self._set_log_levels(options)

        dag_info = _DAGInfo() if options.get('dags') else None

        log.warning('STARTED generating Course Blocks for %d courses.', len(course_keys))
        self._generate_course_blocks(options, course_keys, dag_info)
        log.warning('FINISHED generating Course Blocks for %d courses.', len(course_keys))

        if dag_info:
            log.critical('DAG data: %s', unicode(dag_info))

    def _set_log_levels(self, options):
        """
        Sets logging levels for this module and the block structure
        cache module, based on the given the options.
        """
        if options.get('verbosity') == 0:
            log_level = logging.CRITICAL
        elif options.get('verbosity') == 1:
            log_level = logging.WARNING
        else:
            log_level = logging.INFO

        if options.get('verbosity') < 3:
            cache_log_level = logging.CRITICAL
        else:
            cache_log_level = logging.INFO

        log.setLevel(log_level)
        cache.logger.setLevel(cache_log_level)

    def _generate_course_blocks(self, options, course_keys, dag_info=None):
        """
        Generates course blocks for the given course_keys per the given options.
        Updates dag_info if provided.
        """

        for course_key in course_keys:
            try:
                log.info('STARTED generating Course Blocks for course: %s.', course_key)
                block_structure = None

                if options.get('enqueue_task'):
                    action = tasks.update_course_in_cache if options.get('force_update') else tasks.get_course_in_cache
                    task_options = {'routing_key': options['routing_key']} if options.get('routing_key') else {}
                    action.apply_async([unicode(course_key)], **task_options)
                else:
                    action = api.update_course_in_cache if options.get('force_update') else api.get_course_in_cache
                    block_structure = action(course_key)

                if dag_info:
                    self._find_and_log_dags(block_structure, course_key, dag_info)

                log.info('FINISHED generating Course Blocks for course: %s.', course_key)
            except Exception as ex:  # pylint: disable=broad-except
                log.exception(
                    'An error occurred while generating course blocks for %s: %s',
                    unicode(course_key),
                    ex.message,
                )

    def _find_and_log_dags(self, block_structure, course_key, dag_info):
        """
        Finds all DAGs within the given block structure.

        Arguments:
            BlockStructureBlockData - The block structure in which to find DAGs.
        """
        for block_key in block_structure.get_block_keys():
            parents = block_structure.get_parents(block_key)
            if len(parents) > 1:
                dag_info.on_dag_found(course_key, block_key)
                log.warning(
                    'DAG alert - %s has multiple parents: %s.',
                    unicode(block_key),
                    [unicode(parent) for parent in parents],
                )


class PrettyDefaultDict(defaultdict):
    """
    Wraps defaultdict to provide a better string representation.
    """
    __repr__ = dict.__repr__


class _DAGBlockTypeInfo(object):
    """
    Class for aggregated DAG data for a specific block type.
    """
    def __init__(self):
        self.num_of_dag_blocks = 0

    def __repr__(self):
        return repr(vars(self))


class _DAGCourseInfo(object):
    """
    Class for aggregated DAG data for a specific course run.
    """
    def __init__(self):
        self.num_of_dag_blocks = 0
        self.dag_data_by_block_type = PrettyDefaultDict(_DAGBlockTypeInfo)

    def __repr__(self):
        return repr(vars(self))

    def on_dag_found(self, block_key):
        """
        Updates DAG collected data for the given block.
        """
        self.num_of_dag_blocks += 1
        self.dag_data_by_block_type[block_key.category].num_of_dag_blocks += 1


class _DAGInfo(object):
    """
    Class for aggregated DAG data.
    """
    def __init__(self):
        self.total_num_of_dag_blocks = 0
        self.total_num_of_dag_courses = 0
        self.dag_data_by_course = PrettyDefaultDict(_DAGCourseInfo)
        self.dag_data_by_block_type = PrettyDefaultDict(_DAGBlockTypeInfo)

    def __repr__(self):
        return repr(vars(self))

    def on_dag_found(self, course_key, block_key):
        """
        Updates DAG collected data for the given block.
        """
        self.total_num_of_dag_blocks += 1
        if course_key not in self.dag_data_by_course:
            self.total_num_of_dag_courses += 1
        self.dag_data_by_course[unicode(course_key)].on_dag_found(block_key)
        self.dag_data_by_block_type[block_key.category].num_of_dag_blocks += 1
