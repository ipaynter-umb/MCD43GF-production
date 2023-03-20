class Test:

    def __init__(self):

        self.blue = 2
        self.red = 4

test = Test()

for attr in test.__dict__.keys():

    print(attr, getattr(test, attr))