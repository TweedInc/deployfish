from functools import wraps
import os
import os.path
import re
import yaml
import sys

import click

from deployfish.aws import build_boto3_session
from deployfish.terraform import (NoSuchStateFile, Terraform, TerraformE)


def needs_config(func):
    """
    Add a fully configured Config() object to the ctx variable for our click function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            args[0].obj['CONFIG'] = Config(
                filename=args[0].obj['CONFIG_FILE'],
                env_file=args[0].obj['ENV_FILE'],
                import_env=args[0].obj['IMPORT_ENV'],
                tfe_token=args[0].obj['TFE_TOKEN']
            )
        except NoSuchStateFile as e:
            click.echo(str(e))
            sys.exit(1)
        else:
            return func(*args, **kwargs)
    return wrapper


class Config(object):

    """
    This class reads our ``deployfish.yml`` file and handles the allowed
    variable substitutions in string values for service entries under the
    ``services:`` section.

    Allowed variable substitutions:

    * ``${terraform.<lookup key>}``:  If we have a ``terraform:`` section
      in our YAML, replace this with the terraform lookup value for
      ``<lookup key>``.

    * ``${env.<environment var>}```:  If the environment variable
      ``<environment var>`` exists in our environment, replace this with
      the value of that environment variable.
    """

    TERRAFORM_RE = re.compile('\$\{terraform.(?P<key>[A-Za-z0-9_]+)\}')
    TERRAFORM_CONCAT_RE = re.compile('\$\{concat\(terraform.(?P<key>[A-Za-z0-9_]+)\)\}')
    ENVIRONMENT_RE = re.compile('\$\{env.(?P<key>.+)\}')

    def __init__(self, filename='deployfish.yml', env_file=None, import_env=False, interpolate=True, tfe_token=None, use_aws_section=True, raw_config=None, boto3_session=None):
        # Load a raw config if it was provided
        if raw_config:
            self.__raw = raw_config
        else:
            self.__raw = self.load_config(filename)

        # Setup our boto3_session here because we might need it when retrieving
        # the terraform file from S3

        self.import_env = import_env
        self.env_file = env_file
        self.tfe_token = tfe_token
        self.environ = None
        self.terraform = None

        if interpolate and 'aws' in self.__raw:
            self.replace_aws()

        if use_aws_section:
            build_boto3_session(self, boto3_session_override=boto3_session)
        else:
            build_boto3_session(boto3_session_override=boto3_session)

        if interpolate:
            if 'terraform' in self.__raw:
                self.replace_terraform()
                if 'workspace' in self.__raw['terraform']:
                    self.terraform = TerraformE(yml=self.__raw['terraform'], api_token=self.tfe_token)
                else:
                    self.terraform = Terraform(yml=self.__raw['terraform'])
            else:
                self.terraform = None
            self.replace()

    @property
    def raw(self):
        return self.__raw

    def load_config(self, filename):
        """
        Read our deployfish.yml file from disk and return it as parsed YAML.

        :param filename: the path to our deployfish.yml file
        :type filename: string

        :rtype: dict
        """
        with open(filename) as f:
            return yaml.load(f)

    def load_env_file(self, env_file):
        if env_file and os.path.isfile(env_file):
            lines = []
            with open(env_file) as f:
                lines = f.readlines()
                # Strip the comments and empty lines
                lines = [x.strip() for x in lines if x.strip() and not x.strip().startswith("#")]
            for line in lines:
                # split on the first "="
                parm = str.split(line, '=', 1)
                if len(parm) == 2:
                    key = parm[0]
                    value = parm[1]
                    self.environ[key] = value

    def load_environ(self):
        for key in os.environ.keys():
            self.environ[key] = os.getenv(key)

    def replace(self):
        """
        Do variable replacement in all strings in the YAML data for
        each listed services under the ``services:`` section.
        """

        sections = ['task', 'service']

        for name in sections:
            plural = "{}s".format(name)
            if plural in self.__raw:
                for section in self.__raw[plural]:
                    replacers = {
                        "{}-name".format(name):section['name']
                    }
                    replacers['environment'] = section.get('environment', 'prod')
                    if 'cluster' in section:
                        replacers['cluster'] = section['cluster']
                    self.environ = {}
                    if 'env_file' in section:
                        self.load_env_file(section['env_file'])
                    if self.env_file:
                        self.load_env_file(self.env_file)
                    if self.import_env:
                        self.load_environ()

                    self.__do_dict(section, replacers)

        # if 'tasks' in self.__raw:
        #     for task in self.__raw['tasks']:
        #         replacers = {
        #             'environment': task.get('environment', 'prod'),
        #             'task-name': task['name']
        #         }
        #         if 'cluster' in task:
        #             replacers['cluster-name'] = task['cluster']
        #         self.environ = {}
        #         if 'env_file' in task:
        #             self.load_env_file(task['env_file'])
        #         if self.env_file:
        #             self.load_env_file(self.env_file)
        #         if self.import_env:
        #             self.load_environ()
        #         # else:
        #         #     self.environ = os.environ
        #
        #         self.__do_dict(task, replacers)
        #
        # if 'services' in self.__raw:
        #     for service in self.__raw['services']:
        #         replacers = {
        #             'environment': service.get('environment', 'prod'),
        #             'service-name': service['name'],
        #             'cluster-name': service['cluster']
        #         }
        #         self.environ = {}
        #         if 'env_file' in service:
        #             self.load_env_file(service['env_file'])
        #         if self.env_file:
        #             self.load_env_file(self.env_file)
        #         if self.import_env:
        #             self.load_environ()
        #         # else:
        #         #     self.environ = os.environ
        #
        #         self.__do_dict(service, replacers)

    def replace_aws(self):
        if not self.environ:
            self.environ = {}
        if self.env_file:
            self.load_env_file(self.env_file)
        if self.import_env:
            self.load_environ()
        self.__do_dict(self.__raw['aws'], {})

    def replace_terraform(self):
        if isinstance(self.__raw['terraform'], dict):
            self.__do_dict(self.__raw['terraform'], {})
        elif isinstance(self.__raw['terraform'], list):
            self.__do_list(self.__raw['terraform'], {})

        for service in self.__raw['services']:
            replacers = {
                'environment': service.get('environment', 'prod'),
                'service-name': service['name'],
                'cluster-name': service['cluster']
            }
            if 'workspace' in self.__raw['terraform']:
                self.__raw['terraform']['workspace'] = self.__raw['terraform']['workspace'].format(**replacers)
            else:
                if isinstance(self.__raw['terraform'], dict):
                    try:
                        self.__raw['terraform']['statefile'] = self.__raw['terraform']['statefile'].format(**replacers)
                    except KeyError:
                        print('Skipping replacers')
                elif isinstance(self.__raw['terraform'], list):
                    for statefile_dict in self.__raw['terraform']:
                        try:
                            statefile_dict['statefile'] = statefile_dict['statefile'].format(**replacers)
                        except KeyError:
                            print('Skipping replacers')

    def __replace(self, raw, key, value, replacers):
        if isinstance(value, dict):
            self.__do_dict(value, replacers)
        elif any(isinstance(value, t) for t in (list, tuple)):
            self.__do_list(value, replacers)
        elif isinstance(value, str):
            self.__do_string(raw, key, value, replacers)

    def __env_replace(self, key, replacers):
        envkey = key.format(**replacers).upper().replace('-', '_')
        value = self.environ.get(envkey, envkey)
        return value

    def __do_string(self, raw, key, value, replacers):
        if self.terraform:
            mc = self.TERRAFORM_CONCAT_RE.search(value)
            m = self.TERRAFORM_RE.search(value)
            if m or mc:
                if m:
                    matcher = m
                else:
                    matcher = mc
                tfvalue = self.terraform.lookup(matcher.group('key'), replacers)
                if isinstance(tfvalue, (list, tuple, dict)):
                    if mc and isinstance(tfvalue, (list, tuple)):
                        tfvalue = ','.join(tfvalue)
                    raw[key] = tfvalue
                    self.__replace(raw, key, tfvalue, replacers)
                    return
                if type(tfvalue) == int:
                    tfvalue = str(tfvalue)
                raw[key] = self.TERRAFORM_RE.sub(tfvalue, value)
                value = raw[key]
        m = self.ENVIRONMENT_RE.search(value)
        if m:
            # TODO: using __env_replace here is risky because of {service-name}
            # and {cluster-name}.  If these have a `-` or a '.' in them, the
            # environment variable name will be treated strangely by the shell
            # or just rejected.
            #
            # In each replacer, we should be replacing [.- ] with _ and then
            # uppercasing the result.
            raw[key] = self.ENVIRONMENT_RE.sub(self.__env_replace(m.group('key'), replacers), value)

    def __do_list(self, raw, replacers):
        for i, value in enumerate(raw):
            self.__replace(raw, i, value, replacers)

    def __do_dict(self, raw, replacers):
        for key, value in raw.items():
            self.__replace(raw, key, value, replacers)

    def get_task(self, task_name):
        """
        Get the full config for the task named ``task_name`` from our
        parsed YAML file.

        :param task_name: the name of an ECS task listed in our YAML
                             file under the ``tasks:`` section
        :type task_name: string

        :rtype: dict
        """
        for task in self.__raw['tasks']:
            if task['name'] == task_name:
                return task
            if 'environment' in task and task['environment'] == task_name:
                return task
        raise KeyError

    def get_service(self, service_name):
        """
        Get the full config for the service named ``service_name`` from our
        parsed YAML file.

        :param service_name: the name of an ECS service listed in our YAML
                             file under the ``services:`` section
        :type service_name: string

        :rtype: dict
        """
        for service in self.__raw['services']:
            if service['name'] == service_name:
                return service
            if 'environment' in service and service['environment'] == service_name:
                return service
        raise KeyError

    def get_section(self, section):
        """
        Return the contents of a whole top level section from our deployfish.yml file.

        :param section: The name of the top level section to search
        :type section: string

        :rtype: dict
        """
        return self.__raw[section]

    def get_section_item(self, section, item_name):
        """
        Get an item from a top level section with 'name' equal to ``item_name``
        from our parsed ``deployfish.yml`` file.

        :param section: The name of the top level section to search
        :type section: string

        :param item: The name of the instance of the section

        :rtype: dict
        """
        if section in self.__raw:
            for item in self.__raw[section]:
                if item['name'] == item_name:
                    return item
        raise KeyError
