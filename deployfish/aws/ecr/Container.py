from deployfish.aws import get_boto3_session


class Container:
    def __init__(self, yml):
        self.ecr = get_boto3_session().client('ecr')
        if yml:
            self.from_yaml(yml)

    def from_yaml(self, yml):
        pass

    def tag_and_push(self):
        pass