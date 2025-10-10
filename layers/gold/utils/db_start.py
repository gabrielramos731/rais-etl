#%%
from layers.gold.utils.db_model import create_schema, create_dimensions, create_facts
from layers.gold.utils.db_config import create_connection

#%%
conn = create_connection()
create_schema(conn, 'dimensional')
create_dimensions(conn, 'dimensional')
create_facts(conn, 'dimensional')

#%%

