===========
ms_apm_etl
===========


ETL process to load and summarize the data on the Agency Performance Model dataset on Kaggle: https://www.kaggle.com/moneystore/agencyperformance


Instructions
============

The package can be installed with pip using the following command:

.. code-block:: bash

    $ git clone https://github.com/MarkStefanovic/mes_apm_etl.git

If pipenv is not installed in the current Python environment, install it with the following command:

.. code-block:: bash

    $ pip install pipenv

Next, install the dependencies:

.. code-block:: bash

    $ cd mes_insurance_etl
    $ pipenv install


Finally, you are able to run the package:

.. code-block:: bash

    $ pipenv run python load.py


REST API
========

A REST API based on the above ETL process (using a remote database) works as follows:

https://mes-insurance-api.herokuapp.com/agencies?format=json

This returns the list of agencies on the agency_dim table in json format.  If you change the format to csv then it will download the data as a csv.

https://mes-insurance-api.herokuapp.com/products?format=json

This returns the list of products on the product_dim table in json format.  If you change the format to csv then it will download the data as a csv.

https://mes-insurance-api.herokuapp.com/accounts-receivable?state=OH&format=json

This returns the list of products on the revenue_fact table in json format.  If you change the format to csv then it will download the data as a csv.

The accounts-receivable endpoint has parameters for the agencyid, state, and product.  If you don't include any parameters it will return data for all agencies, all states, and all products.  If you provide one or more of the parameters then it will only return the rows that meet the criteria.