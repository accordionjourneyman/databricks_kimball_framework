import ast
import sys

class CognitiveComplexityVisitor(ast.NodeVisitor):
    def __init__(self):
        self.complexity = 0
        self.nesting = 0
    
    def _increment(self, increment=1):
        self.complexity += increment + self.nesting
    
    def visit_FunctionDef(self, node):
        self.nesting = 0
        self.generic_visit(node)
    
    visit_AsyncFunctionDef = visit_FunctionDef
    
    def visit_ClassDef(self, node):
        self.generic_visit(node)
    
    def visit_If(self, node):
        self._increment(1)
        self.nesting += 1
        self.generic_visit(node)
        self.nesting -= 1
    
    def visit_For(self, node):
        self._increment(1)
        self.nesting += 1
        self.generic_visit(node)
        self.nesting -= 1
    
    visit_AsyncFor = visit_For
    
    def visit_While(self, node):
        self._increment(1)
        self.nesting += 1
        self.generic_visit(node)
        self.nesting -= 1
    
    def visit_Try(self, node):
        self.generic_visit(node)
        for handler in node.handlers:
            self._increment(1)
            self.visit(handler)
    
    def visit_ExceptHandler(self, node):
        # already counted in visit_Try
        self.generic_visit(node)
    
    def visit_With(self, node):
        self._increment(1)
        self.generic_visit(node)
    
    visit_AsyncWith = visit_With
    
    def visit_BoolOp(self, node):
        # each and/or beyond the first adds complexity
        self._increment(len(node.values) - 1)
        self.generic_visit(node)
    
    def visit_Break(self, node):
        self._increment(1)
    
    def visit_Continue(self, node):
        self._increment(1)
    
    def visit_Return(self, node):
        pass
    
    def generic_visit(self, node):
        super().generic_visit(node)

def calculate_cognitive_complexity(source):
    tree = ast.parse(source)
    visitor = CognitiveComplexityVisitor()
    visitor.visit(tree)
    return visitor.complexity

with open(sys.argv[1], 'r') as f:
    source = f.read()
print(f"Cognitive Complexity: {calculate_cognitive_complexity(source)}")
