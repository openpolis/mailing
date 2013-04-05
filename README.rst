Estrazione degli indirizzi
==========================

with ``ssh root@ovhb1``::

    mysql -uroot op_accesso

    select p.first_name, p.last_name, p.email, l.name 
    into outfile '/tmp/op_siti_subscribers_no_newsletter.csv' FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' 
    from op_profile p, op_location l 
    where l.id=p.location_id and is_active=True and wants_newsletter=False;

    select p.first_name, p.last_name, p.email, l.name 
    into outfile '/tmp/op_siti_subscribers_yes_newsletter.csv' FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' 
    from op_profile p, op_location l where l.id=p.location_id and is_active=True and wants_newsletter=True;


.. code::

    scp root@ovhb1:/tmp/op_siti_subscribers_yes_newsletter.csv data/
    scp root@ovhb1:/tmp/op_siti_subscribers_no_newsletter.csv data/


with ``ssh root@ovhb``::

    workon open_coesione
    django_admin.py exportsubscribers > /tmp/op_assoc_subscribers.csv
    
.. code::
    scp root@ovhb1:/tmp/op_assoc_subscribers.csv data/


Unione
======
Non è sempre necessario, ma a volte occorre unire diversi indirizzari.
Basta usare cat,facendo passare l'output per i filtri ``sort`` e ``uniq``.

.. code::

    pushd data
    cat op_assoc_subscribers.csv op_siti_subscribers_yes_newsletter op_siti_subscribers_no_newsletter | sort | uniq >> nostri_unclean.csv

    
Rimozione blacklist
===================

.. code::

    cat data/nostri_unclean.csv | \
        python remove_blacklist.py --blacklist data/black_list_utenti_openpolis.txt --log=logfile \
        > data/nostri.csv
        
        
Validazione indirizzi
