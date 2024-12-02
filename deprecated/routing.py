import psycopg2 as pg
import os

# Network
NET_DB = 'streets'
NET_USER = 'postgres'
NET_PASS = 'postgres'
NET_HOST = 'localhost'
NET_PORT = 5432

# Establish connection
def open_connection():
    cred = {
        'user': NET_USER,
        'password': NET_PASS,
        'host': NET_HOST,
        'port': NET_PORT,
        'database': NET_DB
    }
    try:
        connection = pg.connect(user=cred['user'],
                                password=cred['password'],
                                host=cred['host'],
                                port=cred['port'],
                                database=cred['database'])
        cursor = connection.cursor()
        print('Connection established...')

        return connection, cursor
    except (Exception, pg.Error) as error:
        print('Connection failed...', error)


# Close connection
def close_connection(connection, cursor):
    if connection:
        cursor.close()
        connection.close()
        print("Connection closed...")


# Fetch data
def fetch(cursor, query):
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        return result
    except (Exception, pg.Error) as error:
        print('Failed to fetch...', error)


def query(origin, destination, cur):
    # Fetch routing
    query = \
        '''
        /* ORIGIN */
        WITH origin AS (
            SELECT ST_GeomFromText('{orig_wkt}', 4326) AS geom
        ),

        /* DESTINATION */
        destination AS (
            SELECT ST_GeomFromText('{dest_wkt}', 4326) AS geom
        ),

        /* Nearest way to point */
        origin_nearest_way AS (
            SELECT ways.the_geom as geom,
                   ways.source as source,
                   ways.target as target
            FROM ways, origin
            ORDER BY ST_Distance(the_geom,
                                 origin.geom,
                                 true) 
            ASC LIMIT 1
        ),

        /* Projection to nearest way */
        origin_projection AS (
            SELECT ST_Line_Locate_Point(origin_nearest_way.geom, origin.geom) AS fraction,
                   ST_Line_Interpolate_Point(origin_nearest_way.geom, 
                    ST_Line_Locate_Point(origin_nearest_way.geom, 
                                         origin.geom)) AS geom
            FROM origin_nearest_way, origin
        ),

        /* Projection distance */
        origin_proj_distance AS (
            SELECT st_makeline(origin.geom, origin_projection.geom) as geom
            FROM origin, origin_projection
        ),

        /* Source & target of nearest way */
        origin_vertices AS (
            SELECT source as id, 
                   ways_vertices_pgr.the_geom as geom,
                   0 as fraction
            FROM origin_nearest_way
            LEFT JOIN ways_vertices_pgr
            ON source = ways_vertices_pgr.id
            UNION
            SELECT target as id, 
                   ways_vertices_pgr.the_geom as geom,
                   1 as fraction
            FROM origin_nearest_way
            LEFT JOIN ways_vertices_pgr
            ON target = ways_vertices_pgr.id
        ),

        /* Nearest source/target to projection */
        origin_nearest_vertex AS (
            SELECT origin_vertices.id as id, 
                   origin_vertices.geom as geom,
                   origin_vertices.fraction as fraction
            FROM origin_vertices, destination
            ORDER BY ST_Distance(origin_vertices.geom,
                                 destination.geom,
                                 true) 
            ASC LIMIT 1
        ),

        /* Linestring between projection and source/target */
        origin_linestring AS (
            SELECT CASE WHEN (SELECT origin_projection.fraction <> origin_nearest_vertex.fraction 
                              FROM origin_projection, origin_nearest_vertex)
            THEN
                (SELECT CASE WHEN (SELECT origin_projection.fraction < origin_nearest_vertex.fraction 
                                   FROM origin_projection, origin_nearest_vertex)
                THEN
                    (SELECT st_line_substring(origin_nearest_way.geom,
                                              origin_projection.fraction,
                                              origin_nearest_vertex.fraction) as geom
                     FROM origin_nearest_way, origin_nearest_vertex, origin_projection)
                ELSE
                    (SELECT st_line_substring(origin_nearest_way.geom,
                                              origin_nearest_vertex.fraction,
                                              origin_projection.fraction) as geom
                     FROM origin_nearest_way, origin_nearest_vertex, origin_projection)
                END)
            ELSE 
                (SELECT ST_GeomFromText('LINESTRING EMPTY') as geom)
            END
        ),

        /* Nearest way to point */
        destination_nearest_way AS (
            SELECT ways.the_geom as geom,
                   ways.source as source,
                   ways.target as target
            FROM ways, destination
            ORDER BY ST_Distance(the_geom,
                                 destination.geom,
                                 true) 
            ASC LIMIT 1
        ),

        /* Projection to nearest way */
        destination_projection AS (
            SELECT ST_Line_Locate_Point(destination_nearest_way.geom, destination.geom) AS fraction,
                   ST_Line_Interpolate_Point(destination_nearest_way.geom, 
                    ST_Line_Locate_Point(destination_nearest_way.geom, 
                                         destination.geom)) AS geom
            FROM destination_nearest_way, destination
        ),

        /* Projection distance */
        destination_proj_distance AS (
            SELECT st_makeline(destination.geom, destination_projection.geom) as geom
            FROM destination, destination_projection
        ),

        /* Source & target of nearest way */
        destination_vertices AS (
            SELECT source as id, 
                   ways_vertices_pgr.the_geom as geom,
                   0 as fraction
            FROM destination_nearest_way
            LEFT JOIN ways_vertices_pgr
            ON source = ways_vertices_pgr.id
            UNION
            SELECT target as id, 
                   ways_vertices_pgr.the_geom as geom,
                   1 as fraction
            FROM destination_nearest_way
            LEFT JOIN ways_vertices_pgr
            ON target = ways_vertices_pgr.id
        ),

        /* Nearest source/target to projection */
        destination_nearest_vertex AS (
            SELECT destination_vertices.id as id, 
                   destination_vertices.geom as geom,
                   destination_vertices.fraction as fraction
            FROM destination_vertices, origin
            ORDER BY ST_Distance(destination_vertices.geom,
                                 origin.geom,
                                 true) 
            ASC LIMIT 1
        ),


        /* Linestring between projection and source/target */
        destination_linestring AS (
            SELECT CASE WHEN (SELECT destination_projection.fraction <> destination_nearest_vertex.fraction 
                              FROM destination_projection, destination_nearest_vertex)
            THEN
                (SELECT CASE WHEN (SELECT destination_projection.fraction < destination_nearest_vertex.fraction 
                                   FROM destination_projection, destination_nearest_vertex)
                THEN
                    (SELECT st_line_substring(destination_nearest_way.geom,
                                              destination_projection.fraction,
                                              destination_nearest_vertex.fraction) as geom
                     FROM destination_nearest_way, destination_nearest_vertex, destination_projection)
                ELSE
                    (SELECT st_line_substring(destination_nearest_way.geom,
                                              destination_nearest_vertex.fraction,
                                              destination_projection.fraction) as geom
                     FROM destination_nearest_way, destination_nearest_vertex, destination_projection)
                END)
            ELSE 
                (SELECT ST_GeomFromText('LINESTRING EMPTY') as geom)
            END
        ),

        route AS (
            SELECT id, ways.the_geom as geom FROM pgr_dijkstra('
                SELECT id,
                       source,
                       target,
                       cost
                FROM ways',
                (SELECT id FROM origin_nearest_vertex), 
                (SELECT id FROM destination_nearest_vertex),
                 FALSE
            ) AS dijkstra
            LEFT JOIN ways
            ON (dijkstra.edge = ways.id)
            ORDER BY seq
        ),

        total as (
            SELECT ST_AsText(geom) as geom FROM route
            union
            SELECT ST_AsText(geom) as geom FROM origin_linestring
            union
            SELECT ST_AsText(geom) as geom FROM destination_linestring
            union
            SELECT ST_AsText(geom) as geom FROM origin_proj_distance
            union
            SELECT ST_AsText(geom) as geom FROM destination_proj_distance
        )
        '''.format(orig_wkt=origin, dest_wkt=destination)
    if origin == destination: return (None, None)
    # recover geometry
    wkt_query = query + \
                '''
                    select st_astext(st_linemerge(st_collect(geom))) as geom
                    from total
                '''
    wkt = fetch(cur, wkt_query)[0][0]

    # recover sequence
    seq_query = query + \
                '''
                    select id
                    from route
                '''
    seq = fetch(cur, seq_query)
    seq = [str(id[0]) for id in seq][:-1]
    seq = '@'.join(seq)

    return wkt, seq