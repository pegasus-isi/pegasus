#!/usr/bin/env python
import sys
import os
import xml.sax.handler
import xml.sax
from optparse import OptionParser
import colorsys

COLORS = [
    "#1b9e77",
    "#d95f02",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
    "#a6761d",
    "#666666",
    "#8dd3c7",
    "#bebada",
    "#fb8072",
    "#80b1d3",
    "#fdb462",
    "#b3de69",
    "#fccde5",
    "#d9d9d9",
    "#bc80bd",
    "#ccebc5",
    "#ffed6f",
    "#ffffb3",
    "#aadd88",
    "#889933",
    "#22bbcc",
    "#d9dbb5"
]

def rgb2hex(r,g,b):
    return "#%02x%02x%02x" % (r*255,g*255,b*255)

# Generate some colors to add to the list
s = .7
for l in [.70, .55]:
    for h in range(0, 101, 10):
        if h == 40: continue
        rgb = colorsys.hls_to_rgb(h/100.0, l, s)
        COLORS.append(rgb2hex(*rgb))

class DAG:
    def __init__(self):
        self.nodes = {}

class Node(object):
    def __init__(self):
        self.id = None
        self.label = None
        self.level = 0
        self.parents = []
        self.children = []
        self.mark = 0
        self.closure = set()

    def renderNode(self, renderer):
        pass

    def renderEdge(self, renderer, parent):
        renderer.renderEdge(parent.id, self.id)

    def __repr__(self):
        return "(%s, %s)" % (self.id, self.label)

class Job(Node):
    def __init__(self):
        Node.__init__(self)
        self.xform = None

    def renderNode(self, renderer):
        if renderer.label_type == 'xform':
            label = self.xform
        elif renderer.label_type == 'id':
            label = "%s" % self.id
        elif renderer.label_type == 'xform-id':
            label = "%s\\n%s" % (self.xform, self.id)
        elif renderer.label_type == 'label-xform':
            label = "%s\\n%s" % (self.label, self.xform)
        elif renderer.label_type == 'label-id':
            label = "%s\\n%s" % (self.label, self.id)
        else:
            label = self.label
        color = renderer.getcolor(self.xform)
        renderer.renderNode(self.id, label, color)

class File(Node):
    def __init__(self):
        Node.__init__(self)

    def renderNode(self, renderer):
        renderer.renderNode(self.id, self.label, fillcolor="#ffed6f", shape="rect")

class Stack:
    def __init__(self):
        self.items = []

    def push(self, item):
        self.items.append(item)

    def pop(self):
        return self.items.pop()

    def peek(self, index=1):
        index = 0 - index
        return self.items[index]

class DAXHandler(xml.sax.handler.ContentHandler):
    """
    This is a DAX file parser
    """
    def __init__(self, files):
        self.files = files
        self.elements = Stack()
        self.last_job = None
        self.dag = DAG()
        self.nodes = self.dag.nodes

    def startElement(self, name, attrs):
        self.elements.push(name)

        if name in ["job","dax","dag"]:
            job = Job()

            job.id = attrs.get("id")
            if job.id is None:
                raise Exception("Invalid DAX: attribute 'id' missing")

            if name == "job":
                job.xform = attrs.get("name")
                if job.xform is None:
                    raise Exception("Invalid DAX: job name missing for job %s" % job.id)
                ns = attrs.get("namespace")
                version = attrs.get("version")
                if ns is not None:
                    job.xform = ns + "::" + job.xform
                if version is not None:
                    job.xform = job.xform + ":" + version
            elif name == "dax":
                job.xform = "pegasus::dax"
            else:
                job.xform = "pegasus::dag"

            job.label = attrs.get("node-label")
            if job.label is None:
                job.label = attrs.get("file")
                if job.label is None:
                    job.label = job.xform

            self.nodes[job.id] = job
            self.last_job = job
        elif name == "uses":
            if not self.files:
                return

            # Ignore uses inside all elements except job
            if self.elements.peek(2) != "job":
                return

            namespace = attrs.get("namespace")
            version = attrs.get("version")
            filename = attrs.get("name") or attrs.get("file")

            if filename is None:
                raise Exception("name attribute missing on <uses>")

            if namespace is not None:
                filename = namespace + "::" + filename

            if version is not None:
                filename = filename + ":" + version

            if filename in self.nodes:
                f = self.nodes[filename]
            else:
                f = File()
                f.id = f.label = filename
                self.nodes[filename] = f

            link = attrs.get("link") or "input"
            link = link.lower()
            if link == "input":
                f.children.append(self.last_job)
                self.last_job.parents.append(f)
            elif link == "output":
                f.parents.append(self.last_job)
                self.last_job.children.append(f)
            elif link == "inout":
                print "WARNING: inout file %s of %s creates a cycle." % (f.id, self.last_job)
                f.children.append(self.last_job)
                f.parents.append(self.last_job)
                self.last_job.parents.append(f)
                self.last_job.children.append(f)
            elif link == "none":
                pass
            else:
                raise Exception("Unrecognized link value: %s" % link)
        elif name == "child":
            self.lastchild = attrs.get("ref")
        elif name == "parent":
            if self.lastchild is None:
                raise Exception("Invalid DAX: <parent> outside <child>")
            pid = attrs.get("ref")
            child = self.nodes[self.lastchild]
            parent = self.nodes[pid]
            child.parents.append(parent)
            parent.children.append(child)

    def endElement(self, name):
        self.elements.pop()
        if name == "child":
            self.lastchild = None

def parse_daxfile(fname, files=False):
    """
    Parse DAG from a Pegasus DAX file.
    """
    handler = DAXHandler(files)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    f = open(fname,"r")
    parser.parse(f)
    f.close()
    return handler.dag

def parse_xform_name(path):
    """
    Parse the transformation name from a submit script. Usually the
    transformation is in a special classad called '+pegasus_wf_xformation'.
    For special pegasus jobs (create_dir, etc.) set the name manually.
    """
    # Handle special cases
    fname = os.path.basename(path)
    if fname.startswith("create_dir_"): return "pegasus::create_dir"
    if fname.startswith("stage_in_"): return "pegasus::stage_in"
    if fname.startswith("stage_out_"): return "pegasus::stage_out"
    if fname.startswith("stage_inter_"): return "pegasus::stage_inter"
    if fname.startswith("stage_worker_"): return "pegasus::stage_worker"
    if fname.startswith("register_"): return "pegasus::register"
    if fname.startswith("clean_up_"): return "pegasus::clean_up"

    # Get it from the submit file
    if os.path.isfile(path):
        f = open(path,'r')
        for line in f.readlines():
            if '+pegasus_wf_xformation' in line:
                return line.split('"')[1]

    # Otherwise, guess the xform by stripping digits from the name
    name = fname.replace(".sub","")
    return "".join(c for c in name if not '0' <= c <= '9')

def parse_dagfile(fname):
    """
    Parse a DAG from a dagfile.
    """
    dagdir = os.path.dirname(fname)
    dag = DAG()
    jobs = dag.nodes
    lastchild = None
    f = open(fname,'r')
    for line in f.readlines():
        line = line.strip()
        if line.startswith("JOB"):
            rec = line.split()
            job = Job()
            if len(rec) < 3:
                raise Exception("Invalid line:",line)
            job.id = rec[1] # Job id
            subfile = rec[2] # submit script
            if not os.path.isabs(subfile):
                subfile = os.path.join(dagdir,subfile)
            job.xform = parse_xform_name(subfile)
            job.label = job.id
            jobs[job.id] = job
        elif line.startswith("PARENT"):
            rec = line.split()
            if len(rec) < 4:
                raise Exception("Invalid line:",line)
            p = jobs[rec[1]]
            c = jobs[rec[3]]
            p.children.append(c)
            c.parents.append(p)
    f.close()

    return dag

def remove_xforms(dag, xforms):
    """
    Remove transformations in the DAG by name
    """
    nodes = dag.nodes
    if len(xforms) == 0:
        return
    for id in nodes.keys():
        node = nodes[id]
        if isinstance(node, Job) and node.xform in xforms:
            print "Removing %s" % job.id
            for p in node.parents:
                p.children.remove(node)
            for c in node.children:
                c.parents.remove(node)
            del nodes[id]

def transitivereduction(dag):
    # Perform a transitive reduction of the DAG to remove redundant edges.

    # First, perform a topological sort of the workflow.
    roots = [n for n in dag.nodes.values() if len(n.parents) == 0]

    L = []

    def visit(n):
        if n.mark == 1:
            raise Exception("Workflow is not a DAG: Node %s is part of a "
                            "cycle. Try without -f or with -s." % n)

        if n.mark == 0:
            n.mark = 1
            for m in n.children:
                visit(m)
            n.mark = 2
            L.insert(0,n)

    # Visit all the roots to create the topo sort
    for r in roots:
        visit(r)

    # Number all the levels of the workflow, which are used
    # to sort the children of each node in topological order.
    for n in L:
        n.level = 0
        for p in n.parents:
            n.level = max(n.level, p.level+1)

    # The topological sort has to be reversed so that the deepest
    # nodes are visited first
    L.reverse()

    # This compares nodes by level for sorting. Note that sorting
    # children has to be done after the topo sort above because
    # the levels haven't been set until all the roots have been
    # visited.
    def lvlcmp(a, b):
        return a.level - b.level

    # This algorithm is due to Goralcikova and Koubek. It is fast and
    # simple, but it takes a lot of memory for large workflows because
    # it computes and stores the transitive closure of each node.
    for v in L:
        # This is to keep track of how many times v has been visited
        # from one of its parents. When this counter reaches the
        # number of parents the node has, then we can remove the closure
        v.mark = 0

        v.closure = set([v])

        # We need to sort the children in topological order, otherwise the
        # reduction won't work properly. Sorting by level should produce
        # a valid topological ordering.
        v.children.sort(lvlcmp)

        # Compute the transitive closure and identify redundant edges
        reduced = []
        for w in v.children:

            w.mark += 1

            if w in v.closure:
                # If it is already in the closure, then it is not needed
                sys.stderr.write("Removing %s -> %s\n" % (v.label, w.label))
            else:
                v.closure = v.closure.union(w.closure)
                reduced.append(w)

            # Once w has been visited by all its parents we can clear
            # its closure.
            if len(w.parents) == w.mark:
                w.closure = None

        # Another optimization. If v has no parents, then
        # we don't need to save its closure at all.
        if len(v.parents) == 0:
            v.closure = None

        # Now remove the edges
        v.children = reduced

    return dag



class emit_dot(object):
    """Write a DOT-formatted diagram.
    Options:
        label_type: What attribute to use for labels
        outfile: The file name to write the diagam out to.
        width: The width of the diagram
        height: The height of the diagram
    """

    def __init__(self, dag, label_type="label", outfile="/dev/stdout", width=None, height=None):
        self.label_type = label_type

        self.next_color = 0 # Keep track of next color
        self.colors = {} # Keep track of transformation names to assign colors

        self.out = open(outfile, 'w')
        # Render the header
        self.out.write("digraph dag {\n")
        if width and height:
            self.out.write('    size="%0.1f,%0.1f"\n' % (width, height))
        self.out.write('    ratio=fill\n')
        self.out.write('    node [style=filled,color="#444444",fillcolor="#ffed6f"]\n')
        self.out.write('    edge [arrowhead=normal,arrowsize=1.0]\n\n')

        # Render nodes
        for n in dag.nodes.itervalues():
            n.renderNode(self)

        # Render edges
        for p in dag.nodes.itervalues():
            for c in p.children:
                c.renderEdge(self, p)

        self.out.write("}\n")
        self.out.close()

    def getcolor(self, item):
        "Get the color for xform"
        if item not in self.colors:
            self.colors[item] = COLORS[self.next_color]
            # We use the modulus just in case we run out of colors
            self.next_color = (self.next_color + 1) % len(COLORS)
        return self.colors[item]

    def renderNode(self, id, label, fillcolor, color="#000000", shape="ellipse"):
        self.out.write('    "%s" [shape=%s,color="%s",fillcolor="%s",label="%s"]\n' % 
                       (id, shape, color, fillcolor, label))

    def renderEdge(self, parentid, childid, color="#000000"):
        self.out.write('    "%s" -> "%s" [color="%s"]\n' % (parentid, childid, color))

def main():
    labeloptions = ["label","xform","id","xform-id","label-xform","label-id"]
    labeloptionsstring = ", ".join("'%s'" % l for l in labeloptions)
    usage = "%prog [options] FILE"
    description = """Parses FILE and generates a DOT-formatted
graphical representation of the DAG. FILE can be a Condor
DAGMan file, or a Pegasus DAX file."""
    parser = OptionParser(usage=usage,description=description)
    parser.add_option("-s", "--nosimplify", action="store_false",
        dest="simplify", default=True,
        help="Do not simplify the graph by removing redundant edges. [default: False]")
    parser.add_option("-l", "--label", action="store", dest="label", default="label",
        help="What attribute to use for labels. One of %s. "
             "For 'label', the transformation is used for jobs that have no node-label. "
             "[default: label]" % labeloptionsstring)
    parser.add_option("-o", "--output", action="store",
        dest="outfile", metavar="FILE", default="/dev/stdout",
        help="Write output to FILE [default: stdout]")
    parser.add_option("-r", "--remove", action="append",
        dest="remove", metavar="XFORM", default=[],
        help="Remove jobs from the workflow by transformation name")
    parser.add_option("-W", "--width", action="store", dest="width",
        type="float", default=None, help="Width of the digraph")
    parser.add_option("-H", "--height", action="store", dest="height",
        type="float", default=None, help="Height of the digraph")
    parser.add_option("-f", "--files", action="store_true",
        dest="files", default=False,
        help="Include files. This option is only valid for DAX files. [default: false]")

    (options, args) = parser.parse_args()

    if options.width and options.height:
        pass
    elif options.width or options.height:
        parser.error("Either both --width and --height or neither")

    if options.label not in labeloptions:
        parser.error("--label must be one of %s" % labeloptionsstring)

    if len(args) < 1:
        parser.error("Please specify FILE")

    if len(args) > 1:
        parser.error("Invalid argument")

    dagfile = args[0]
    if dagfile.endswith(".dag"):
        dag = parse_dagfile(dagfile)
    else:
        dag = parse_daxfile(dagfile, options.files)

    remove_xforms(dag, options.remove)

    if options.simplify:
        dag = transitivereduction(dag)

    emit_dot(dag, options.label, options.outfile, options.width, options.height)

if __name__ == '__main__':
    main()

