from .node import CoreNode

class DiscoveryNode(CoreNode):
    def __init__(self):
        super().__init__()
        self.protocols[0].continuous_discovery = False
