
.. _quickstart:

==========
Quickstart
==========

Once you have access to a machine with HTCondor and Pegasus installed (see
:ref:`installation` if you need to do a local deployment). Create a 
file with the following contents named ``helloworld.py``:

.. code-block:: python

    #!/usr/bin/env python3
    import logging
    import sys
    
    from Pegasus.api import *
    
    logging.basicConfig(level=logging.DEBUG)
    
    # --- Transformations ----------------------------------------------------------
    echo = Transformation(
            "echo", 
            pfn="/bin/echo",
            site="condorpool",
        )
      
    tc = TransformationCatalog()\
            .add_transformations(echo)
    
    # --- Workflow -----------------------------------------------------------------
    try:
        Workflow("hello-world")\
            .add_jobs(
                Job(echo)
                    .add_args("Hello World")
                    .set_stdout("hello.out")
            ).add_transformation_catalog(tc)\
            .plan(submit=True)
    except PegasusClientError as e:
        print(e.output)


Then generate and submit the workflow in one go with:

.. code-block:: bash

    $ python3 helloworld.py




