.. Remus documentation master file, created by
   sphinx-quickstart on Wed Jan  4 21:28:02 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Remus Dynamic Pipeline Engine
=============================


Manager Interface
=================

.. autoclass:: remus.manage.Config
    :members:


.. autoclass:: remus.manage.Manager
    :members:

Target Classes
==============

.. autoclass:: remus.LocalSubmitTarget
    :members:
    :inherited-members:

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
