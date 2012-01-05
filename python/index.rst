.. Remus documentation master file, created by
   sphinx-quickstart on Wed Jan  4 21:28:02 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Remus's documentation!
=================================

Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Manager Interface
=================

.. autoclass:: remus.manage.Manager
    :members:

Target Classes
==============

.. autoclass:: remus.SubmitTarget
    :members:
    :inherited-members:
    
.. autoclass:: remus.Target
    :members:

Table Interfaces
================

.. autoclass:: remus.db.table.ReadTable
    :members: __iter__
    

.. autoclass:: remus.db.table.WriteTable
    :members: 