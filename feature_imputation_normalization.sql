-------------------------------------------------------------------------------------------------------------------------------
--               Feature imputation to fill-in NULL values and normalization                                               --
--                          Srivatsan Ramanujam<vatsan.cs@utexas.edu>                                            --
-------------------------------------------------------------------------------------------------------------------------------

create schema imputationdemo;

-----------------------------------------------------------------------------------------------------
-- 1) Define UDF for feature imputation
-----------------------------------------------------------------------------------------------------

drop function if exists imputationdemo.impute_missing_values(
    text, 
    text, 
    text[], 
    text, 
    text, 
    boolean, 
    text
) cascade;
create or replace function imputationdemo.impute_missing_values(
    table_schema text, 
    table_name text, 
    exclude_features text[], 
    id_column text,
    label_column text,
    include_constant_term boolean,
    output_table text
)
returns text
as
$$
    import plpy
    import uuid
    #Obtain unique identifier for temp tables
    UNIQUE_IDENTIFIER = str(uuid.uuid4()).replace('-','_')[:20]

    #If label column is specified, don't include it in the list of values to normalize/impute
    excluded_columns = tuple(exclude_features) if not label_column else tuple(exclude_features + [label_column])

    #1) Collect list of column names corresponding to the features.
    sql = """
        select 
            array_agg(column_name order by column_name) as features_names
        from
        (
            select 
                column_name::text
            from 
                information_schema.columns
            where 
                table_schema = '{table_schema}' and 
                table_name = '{table_name}' and
                column_name not in {excluded_columns}
            group by 
                column_name
        )tbl
    """.format(
        table_schema = table_schema,
        table_name = table_name,
        excluded_columns = excluded_columns
    )

    results = plpy.execute(sql)
    feature_names = results[0].get('features_names')

    #2) Compute mean & stddev for each column. Collect these in two dicts.
    #Create table to persist values in disk
    mean_stddev_sql_w_creat_tbl = """
    drop table if exists mean_stddev_stats_{UUID} cascade;
    create temp table mean_stddev_stats_{UUID}
    (    
        column_name text,
        avg double precision,
        stddev double precision
    ) distributed randomly;
    """.format(
        UUID = UNIQUE_IDENTIFIER
    )

    plpy.execute(mean_stddev_sql_w_creat_tbl)

    #Insert template for computing mean & stddev of all features.
    mean_stddev_template = """
        insert into mean_stddev_stats_{UUID}
        select 
            '{column_name}' as column_name, 
            avg({column_name}), 
            stddev({column_name})
        from 
            {table_schema}.{table_name}
    """
    mean_stddev_template_rpt = []

    for feature in feature_names:
        mean_stddev_template_rpt.append(
                mean_stddev_template.format(
                    column_name = feature, 
                    table_schema = table_schema,
                    table_name = table_name,
                    UUID = UNIQUE_IDENTIFIER
                )
            ) 

    #Insert values one at a time now
    for insert_stmt in mean_stddev_template_rpt:
        plpy.execute(insert_stmt)

    #3) Prepare SQL to insert constant_term if specified in the input arguments
    constant_term_sql = """
    """
    if(include_constant_term):
        if(label_column):
            constant_term_sql = """
            union all
            select 
                {id_column},
                'constant_term' as feat_name,
                1 as feat,
                1 as feat_normalized,
                {label_column}
            from 
                {table_schema}.{table_name}
            """.format(
                id_column = id_column,
                table_schema = table_schema,
                table_name = table_name,
                label_column = label_column
            )
        else:
            constant_term_sql = """
            union all
            select 
                {id_column},
                'constant_term' as feat_name,
                1 as feat,
                1 as feat_normalized
            from 
                {table_schema}.{table_name}
            """.format(
                id_column = id_column,
                table_schema = table_schema,
                table_name = table_name
            )

    #Only use those features who have non-zero variance        
    sql = """
        select
            array_agg(column_name order by column_name) as features_names_nonzero_variance
        from
        (
            select
                column_name
            from
                mean_stddev_stats_{UUID}
            where
                stddev > 0
        )q;
    """.format(
        UUID = UNIQUE_IDENTIFIER
    )                
    feature_names_nonzero_variances = plpy.execute(sql)[0].get('features_names_nonzero_variance')

    #4) Now for every row, subtract the mean and divide by stddev
    sql_imputer_normalizer = """
    """

    #If no label column is supplied as input
    if(not label_column):
        sql_imputer_normalizer = """
        drop table if exists {output_table} cascade;
        create table {output_table}
        as
        (
            select 
                {id_column},
                array_agg(feat_name order by feat_name) as feat_name_vect,
                array_agg(feat order by feat_name) as feat_vect,
                array_agg(feat_normalized order by feat_name) as feat_vect_normalized
            from
            (
                select 
                    {id_column},
                    feat_name,
                    feat,
                    (coalesce(feat,avg) - avg)/stddev as feat_normalized
                from
                (
                    select 
                        t1.{id_column},
                        unnest(t2.feat_name_vect) as feat_name,
                        unnest(t1.feat_vect) as feat,
                        unnest(t2.avg_vect) as avg,
                        unnest(t2.stddev_vect) as stddev
                    from
                    (
                        select 
                            {id_column},
                            ARRAY[{feature_names}] as feat_vect 
                        from 
                            {table_schema}.{table_name}
                    )t1,
                    (
                        select 
                            array_agg(column_name order by column_name) as feat_name_vect,
                            array_agg(avg order by column_name) as avg_vect,
                            array_agg(stddev order by column_name) as stddev_vect
                        from 
                            mean_stddev_stats_{UUID}
                        where 
                            column_name not in {excluded_columns} and
                            -- Disregard features with zero variance
                            stddev > 0
                    )t2    
                ) tbl1
                --SQL insert for including the constant term
                {constant_term_sql}
            ) tbl2
            group by 
                {id_column}
        ) distributed by ({id_column});        
        """.format(
            id_column = id_column,
            feature_names = ','.join(feature_names_nonzero_variances),
            table_schema = table_schema,
            table_name = table_name,
            output_table = output_table,
            excluded_columns = excluded_columns,
            constant_term_sql = constant_term_sql,
            UUID = UNIQUE_IDENTIFIER
        )
    else:
        sql_imputer_normalizer = """
        drop table if exists {output_table} cascade;
        create table {output_table}
        as
        (
            select 
                {id_column},
                array_agg(feat_name order by feat_name) as feat_name_vect,
                array_agg(feat order by feat_name) as feat_vect,
                array_agg(feat_normalized order by feat_name) as feat_vect_normalized,
                {label_column}
            from
            (
                select 
                    {id_column},
                    feat_name,
                    feat,
                    (coalesce(feat,avg) - avg)/stddev as feat_normalized,
                    {label_column}
                from
                (
                    select 
                        t1.{id_column},
                        unnest(t2.feat_name_vect) as feat_name,
                        unnest(t1.feat_vect) as feat,
                        unnest(t2.avg_vect) as avg,
                        unnest(t2.stddev_vect) as stddev,
                        t1.{label_column}
                    from
                    (
                        select 
                            {id_column},
                            ARRAY[{feature_names}] as feat_vect,
                            {label_column} 
                        from 
                            {table_schema}.{table_name}
                    )t1,
                    (
                        select 
                            array_agg(column_name order by column_name) as feat_name_vect,
                            array_agg(avg order by column_name) as avg_vect,
                            array_agg(stddev order by column_name) as stddev_vect
                        from 
                            mean_stddev_stats_{UUID}
                        where 
                            column_name not in {excluded_columns} and
                            -- Disregard features with zero variance
                            stddev > 0                            
                    )t2    
                ) tbl1
                --SQL insert for including the constant term
                {constant_term_sql}
            ) tbl2
            group by {id_column}, {label_column}
        ) distributed by ({id_column});        
        """.format(
            id_column = id_column,
            feature_names = ','.join(feature_names_nonzero_variances),
            table_schema = table_schema,
            table_name = table_name,
            output_table = output_table,
            excluded_columns = excluded_columns,
            label_column = label_column,
            constant_term_sql = constant_term_sql,
            UUID = UNIQUE_IDENTIFIER
        )
    plpy.execute(sql_imputer_normalizer)

    #Drop any temporary tables
    sql  = """
        drop table if exists mean_stddev_stats_{UUID};
    """.format(
        UUID = UNIQUE_IDENTIFIER
    )
    plpy.execute(sql)

    return """
        Created normalized features table: {output_table}
    """.format(
        output_table = output_table
    )
$$language plpythonu;

--b)When input is a table of columns and the columns have to be imputed, normalized
drop function if exists imputationdemo.impute_missing_values(
        text,
        text,
        text,
        text[],
        text
);

create or replace function imputationdemo.impute_missing_values(
    table_schema text,
    table_name text,
    id_column text,
    exclude_columns text[],
    output_table text
)
returns text
as
$$
    import plpy
    import uuid
    #Obtain unique identifier for temp tables
    UNIQUE_IDENTIFIER = str(uuid.uuid4()).replace('-','_')[:20]    
    sql = """
        drop table if exists mean_stddev_{UUID};
        create temp table mean_stddev_{UUID}
        (
            column_name text,
            mean float8,
            stddev float8
        ) distributed randomly;
    """.format(
        UUID = UNIQUE_IDENTIFIER
    )
    plpy.execute(sql)

    sql = """
        select 
            column_name, 
            data_type 
        from 
            information_schema.columns 
        where 
            table_schema = '{table_schema}' and 
            table_name = '{table_name}' and

            data_type in (
                'integer',
                'double precision'
            ) and
            column_name not in (
                '{id_column}'
            ) and
            column_name not in '{exclude_columns}'
        group by column_name, data_type
        order by column_name;        
    """.format(
        table_schema = table_schema,
        table_name = table_name,
        id_column = id_column,
        exclude_columns = str(tuple(exclude_columns))
    )

    result = plpy.execute(sql)
    columns = [r['column_name'] for r in result]
    plpy.info(sql)
    plpy.info(str(columns))
    for col in columns:
        sql = """
            insert into mean_stddev_{UUID}
            select *
            from
            (
                select 
                    '{column_name}',
                    avg({column_name}),
                    stddev({column_name})
                from
                    {table_schema}.{table_name}
            )q
            --Disregard columns with zero variance
            where stddev !=0 ;
        """.format(
            column_name = col,
            table_schema = table_schema,
            table_name = table_name,
            UUID = UNIQUE_IDENTIFIER
        )
        plpy.execute(sql)

    sql = """
        select 
            *
        from   
            mean_stddev_{UUID}
    """.format(
        UUID = UNIQUE_IDENTIFIER
    ) 
    result = plpy.execute(sql)   

    mean_std_dict = {}
    for r in result:
        mean_std_dict[r['column_name']]= {'mean':r['mean'], 'stddev':r['stddev']}

    cols = """
        (coalesce({column_name}, {mean}) - {mean})/{stddev} as {column_name}_normalized
    """

    result = []
    for col in columns:
        result.append(cols.format(
                column_name = col,
                mean = mean_std_dict[col]['mean'],
                stddev = mean_std_dict[col]['stddev']
            )
        )

    plpy.info(result)

    sql = """
        drop table if exists {output_table};
        create table {output_table}
        as
        (
            select
                {id_column},
                {place_holder}
            from 
                {table_schema}.{table_name}
        ) distributed randomly;
    """.format(
        id_column = id_column,
        place_holder = ','.join(result),
        table_schema = table_schema,
        table_name = table_name,
        output_table = output_table
    )
    plpy.info(sql)
    plpy.execute(sql)
    return '{output_table} created successfully'.format(output_table = output_table)
$$ language plpythonu;


-----------------------------------------------------------------------------------------------------
-- 2) Sample invocations
-----------------------------------------------------------------------------------------------------

--a) Invoke imputation UDF when input table
select imputationdemo.impute_missing_values(
    'input_schema',
    'input_table',
    -- Columns to exclude
    ARRAY[
        'id', 
        'multi_class_label',
        'year'
    ],
    'id', -- id column
    'multi_class_label', -- label column
    TRUE, -- whether to include a constant term (if running regression)
    'imputationdemo.input_table_imputed' -- output table name
);

--b) Invoke imputation & normalization UDF when input table is a collection of columns
select
    imputationdemo.impute_missing_values(
        'input_schema',
        'input_table',
        'id',
        -- Columns to exclude
        ARRAY[
            'id', 
            'multi_class_label',
            'year'
        ],        
        'imputationdemo.input_table_imputed'
    );

-----------------------------------------------------------------------------------------------------
-- 
-----------------------------------------------------------------------------------------------------   