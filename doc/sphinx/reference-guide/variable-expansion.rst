
.. _variable-expansion:

==================
Variable Expansion
==================

Pegasus Planner supports notion of variable expansions in the Abstract
Workflow and the catalog files along the same lines as bash variable
expansion works. This is often useful, when you want paths in your
catalogs or profile values in the abstract workflow to be picked up
from the environment. An error is thrown if a variable cannot be expanded.

To specify a variable that needs to be expanded, the syntax is
${VARIABLE_NAME} , similar to BASH variable expansion. An important
thing to note is that the variable names need to be enclosed in curly
braces. For example

::

    ${FOO}  - will be expanded by Pegasus
    $FOO    - will NOT be expanded by Pegasus.

Also variable names are case sensitive.

Some examples of variable expansion are illustrated below:

-  **Abstract Workflow**

   A job in the workflow file needs to have a globus profile key project
   associated and the value has to be picked up (per user) from user
   environment.

   .. tabs::

    .. code-tab:: yaml YAML Snippet

        jobs:
        - type: job
          namespace: diamond
          version: '4.0'
          name: preprocess
          id: ID0000001
          arguments: [-a, preprocess, -T, '60', -i, f.a, -o, f.b1, f.b2]
          uses:
          - {lfn: f.a, type: input}
          - {lfn: f.b2, type: output, stageOut: true, registerReplica: true}
          profiles:
            globus: {project: ${PROJECT}}

    .. code-tab:: xml XML Snippet

        <job id="ID0000001" namespace="diamond" name="preprocess" version="4.0">
                <argument>-a preprocess -T60 -i <file name="f.a"/> -o <file name="f.b1"/> <file name="f.b2"/></argument>
                <uses name="f.a" link="input"/>
                <uses name="f.b1" link="output"/>
                <uses name="f.b2" link="output"/>
                <profile namespace="globus" key="project">${PROJECT}</profile>
        </job>

-  **Site Catalog**

   In the site catalog, the site catalog entries are templated, where
   paths are resolved on the basis of values of environment variables.
   For example, below is a templated entry for a local site where $PWD
   is the working directory from where pegasus-plan is invoked.

   .. tabs::

    .. code-tab:: yaml YAML Snippet

        sites:
        - name: local
          arch: x86_64
          os.type: linux
          directories:
          - type: sharedScratch
            path: ${PWD}/LOCAL/shared-scratch
            fileServers:
            - {url: 'file://${PWD}/LOCAL/shared-scratch', operation: all}
          - type: localStorage
            path: ${PWD}/LOCAL/shared-storage
            fileServers:
            - {url: 'file://${PWD}/LOCAL/shared-storage', operation: all}

    .. code-tab:: xml  XML Snippet

      <site  handle="local" arch="x86_64" os="LINUX" osrelease="" osversion="" glibc="">
         <directory  path="${PWD}/LOCAL/shared-scratch" type="shared-scratch" free-size="" total-size="">
            <file-server  operation="all" url="file:///${PWD}/LOCAL/shared-scratch">
            </file-server>
         </directory>
         <directory  path="${PWD}/LOCAL/shared-storage" type="shared-storage" free-size="" total-size="">
            <file-server  operation="all" url="file:///${PWD}/LOCAL/shared-storage">
            </file-server>
         </directory>
         <profile namespace="env" key="PEGASUS_HOME">/usr</profile>
         <profile namespace="pegasus" key="clusters.num" >1</profile>
      </site>

-  **Replica Catalog**

   The input file locations in the Replica Catalog can be resolved based
   on values of environment variables.

   .. tabs::

    .. code-tab:: yaml YAML Snippet

        replicas:
          - lfn: input.txt
            pfns:
              - {site: local, pfn: 'http://${HOSTNAME}/pegasus/input/input.txt'}

    .. code-tab:: text File Snippet
      # File Based Replica Catalog
      production_200.conf file://${PWD}/production_200.conf site="local"

   ..

   .. note::

      Variable expansion is only supported for YAML and File based Replica
      Catalog, not Regex or other file based formats.

-  **Transformation Catalog**

   Similarly paths in the transformation catalog or profile values can
   be picked up from the environment i.e environment variables OS , ARCH
   and PROJECT are defined in user environment when launching
   pegasus-plan.

   .. tabs::

    .. code-tab:: yaml YAML Snippet

        transformations:
        - namespace: pegasus
          name: keg
          sites:
          - {name: isi,
             pfn: /path/to/keg,
             type: installed,
             arch:${ARCH},
             os: ${OS}}
          profiles:
            globus: {project: ${PROJECT}}


    .. code-tab:: text Text Snippet

      # Snippet from a Text Based Transformation Catalog
      tr pegasus::keg{
          site obelix {
              profile globus "project" "${PROJECT}"
              pfn "/usr/bin/pegasus-keg"
              arch "${ARCH}"
              os "${OS}"
              type "INSTALLED"
          }
      }
