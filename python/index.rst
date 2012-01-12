.. Remus documentation master file, created by
   sphinx-quickstart on Wed Jan  4 21:28:02 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Remus Dynamic Pipeline Engine
=============================


Manager Interface
=================

.. automodule:: remus.manage
	:members:

Remus Database
===============

.. automodule:: remus.db
	:members: connect, TableRef, DBBase, FileDB


Target Classes
==============

------------
SubmitTarget
------------
.. autoclass:: remus.SubmitTarget
    :members:

-----------------
LocalSubmitTarget
-----------------
.. autoclass:: remus.LocalSubmitTarget
    :members:
    :inherited-members:

------
Target
------
.. autoclass:: remus.Target
    :members:

-----------
TableTarget
-----------
.. autoclass:: remus.TableTarget
    :members: 

---------
MapTarget
---------
.. autoclass:: remus.MapTarget
    :members:

-----------
RemapTarget
-----------
.. autoclass:: remus.RemapTarget
    :members:


Table Interfaces
================

.. autoclass:: remus.db.table.ReadTable
    :members:
    

.. autoclass:: remus.db.table.WriteTable
    :members: 
