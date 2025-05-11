RPA Land Use Change Data Viewer
=============================

A Python package for analyzing and visualizing land use projection data from the USDA Forest Service Resources Planning Act (RPA) Assessment.

About the RPA Assessment
------------------------

The Resources Planning Act (RPA) Assessment is a report prepared in response to the mandate in the 1974 Forest and Rangeland Renewable Resources Planning Act. The 2020 RPA Assessment provides a comprehensive analysis of the status, trends, and projected future of U.S. forests, forest product markets, rangelands, water, biodiversity, outdoor recreation, and the effects of socioeconomic and climatic change upon these resources.

Features
--------

- Interactive Streamlit dashboard for data exploration
- Comprehensive analysis of urbanization trends and forest transitions
- Modular architecture for data access and analysis
- Semantic layer creation for advanced analytics
- Command-line tools for data management

Installation
-----------

Install from PyPI:

.. code-block:: bash

    pip install rpa-landuse

Or with optional AI analysis features:

.. code-block:: bash

    pip install rpa-landuse[ai]

For development:

.. code-block:: bash

    pip install rpa-landuse[dev]

Quick Start
----------

Run the interactive dashboard:

.. code-block:: bash

    rpa-viewer app

Initialize the database:

.. code-block:: bash

    rpa-viewer init-db

Create semantic layers for analysis:

.. code-block:: bash

    rpa-viewer semantic-layers

Documentation
------------

For full documentation, visit the `GitHub repository <https://github.com/mihiarc/rpa-landuse>`_.

License
-------

This project is licensed under the MIT License. 