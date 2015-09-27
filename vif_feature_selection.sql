-------------------------------------------------------------------------------------------------------------------------------
--                                      Feature Selection Using ViF                                                          --
--                               Detect and remove multi-collinearity using variance inflation factor                        --
--                          Srivatsan Ramanujam<vatsan.cs@utexas.edu>                                             --
-------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------
-- 1) Define UDF to compute ViF scores
-- Note: Please ensure your input table has NULL values imputed with mean/median or else
-- MADlib will dis-regard input rows containing NULLs, while performing regression
-----------------------------------------------------------------------------------------------------

create schema vifdemo;
---------------------------------------------------------------------------------------------
--a) Selecting array slices (filtering out an element as a specified index)
---------------------------------------------------------------------------------------------

--Note, index here are postgres based index (ranges from 1 to N)
drop function if exists vifdemo.filter_array_at_index(text[], int);
create or replace function vifdemo.filter_array_at_index(
    arr text[],
    indx int
)
returns text[]
as
$$
    indx_python = indx - 1
    return arr[:indx_python]+arr[indx_python+1:]
$$language plpythonu;

drop function if exists vifdemo.filter_array_at_index(int[], indx int);
create or replace function vifdemo.filter_array_at_index(
    arr int[],
    indx int
)
returns int[]
as
$$
    indx_python = indx - 1
    return arr[:indx_python]+arr[indx_python+1:]
$$language plpythonu;

drop function if exists vifdemo.filter_array_at_index(float8[], indx int);
create or replace function vifdemo.filter_array_at_index(
    arr float8[],
    indx int
)
returns float[]
as
$$
    indx_python = indx - 1
    return arr[:indx_python]+arr[indx_python+1:]
$$language plpythonu;

---------------------------------------------------------------------------------------------
--b) When input table has id_column, independent_variables as columns, 
--   dependent_variable as a column
---------------------------------------------------------------------------------------------

drop function if exists vifdemo.feature_selection_w_vif(
    text, 
    text, 
    text, 
    text, 
    text[], 
    float, 
    int,
    text
) cascade; 

create function vifdemo.feature_selection_w_vif(
    input_table_name text,
    input_table_schema text,
    id_column text,
    label_column text,
    exclude_columns text[],
    vif_threshold float,
    num_steps int,
    vif_results_table text
)
returns text
as
$$
    import plpy
    from operator import itemgetter
    import uuid
    #Obtain unique identifier for temp tables
    UNIQUE_IDENTIFIER = str(uuid.uuid4()).replace('-','_')[:20]    

    #1) Extract features name from the information schema
    sql = """
        select 
            array_agg(column_name::text order by ordinal_position) as feature_names
        from 
            information_schema.columns
        where 
            table_schema = '{input_table_schema}' and
            table_name = '{input_table_name}' and 
            column_name not in {columns_to_exclude}
    """.format(
           input_table_name = input_table_name,
           input_table_schema = input_table_schema,
           columns_to_exclude = str(exclude_columns + [id_column, label_column]).replace('[','(').replace(']',')')
    )

    feature_names = plpy.execute(sql)[0]['feature_names']
    selected_features_mask = [0 for x in range(len(feature_names))]

    #2) For each feature, treat it as the target variable and regress against the remaining variables
    # to compute its VIF score.
    vif_fselect_table_sql = """
        drop table if exists vif_fselection_{UUID} cascade;
        create temp table vif_fselection_{UUID}
        as
        (
            select 
                {id_column} as id,
                ARRAY[{features_arr}] as feature_names,
                ARRAY[{feature_names}] as features
            from 
                {input_table_schema}.{input_table_name}
        ) distributed by (id);
    """.format(
        id_column = id_column,
        input_table_schema = input_table_schema,
        input_table_name = input_table_name,
        features_arr = str(feature_names).replace('[','').replace(']',''),
        feature_names = ','.join(feature_names),
        UUID = UNIQUE_IDENTIFIER
    )
    plpy.execute(vif_fselect_table_sql)

    #Call the overloaded function to compute ViF
    sql = """
        select
            vifdemo.feature_selection_w_vif(
                '{input_table}',
                '{id_column}',
                '{feature_names_column}',
                '{feature_values_column}',
                {vif_threshold},
                {num_steps},
                '{vif_results_table}'
            );
    """.format(
        input_table = 'vif_fselection_{UUID}'.format(UUID=UNIQUE_IDENTIFIER),
        id_column = 'id',
        feature_names_column = 'feature_names',
        feature_values_column = 'features',
        vif_threshold = vif_threshold,
        num_steps = num_steps,
        vif_results_table = vif_results_table
    )
    results = plpy.execute(sql)[0]
    return results
$$language plpythonu;

---------------------------------------------------------------------------------------------
--c) Overloaded function for scenarios where input table is of the form
-- || id | features_names_arr[] | feature_values_arr[] | label_column ||
---------------------------------------------------------------------------------------------

drop function if exists vifdemo.feature_selection_w_vif(
    text,
    text,
    text,
    text,
    float,
    int,
    text
);

create or replace function vifdemo.feature_selection_w_vif(
    input_table text,
    id_column text,
    feature_names_column text,
    feature_values_column text,
    vif_threshold float,
    num_steps int,
    vif_results_table text
)
returns text
as
$$
    import plpy
    from operator import itemgetter
    import uuid
    #Obtain unique identifier for temp tables
    UNIQUE_IDENTIFIER = str(uuid.uuid4()).replace('-','_')[:20]  

    #1) Create a temp table to iteratively remove features with high multi-collinearity
    sql = """
        drop table if exists vif_fselection_tbl_{UUID};
        create temp table vif_fselection_tbl_{UUID}
        as
        (
            select
                {id_column} as id,
                {feature_names_column} as feature_names,
                {feature_values_column} as features
            from
                {input_table}
        ) distributed by (id);
    """.format(
        id_column = id_column,
        feature_names_column = feature_names_column,
        feature_values_column = feature_values_column,
        input_table = input_table,
        UUID = UNIQUE_IDENTIFIER
    )
    plpy.execute(sql)

    #2) Create a table to hold VIF results
    vif_create_tbl_sql = """
        drop table if exists {vif_results_table} cascade;
        create table {vif_results_table}
        (
            step int,
            feature_w_max_vif text,
            vif float,
            discarded boolean
        ) distributed randomly;
    """.format(
        vif_results_table = vif_results_table
    )
    plpy.execute(vif_create_tbl_sql);

    multicollinearity_exists = True
    step = 0
    while multicollinearity_exists and step < num_steps:
        plpy.info("""Step: {0} of {1}""".format(step, num_steps-1))
        sql = """
            select 
                feature_names
            from 
                vif_fselection_tbl_{UUID}
            limit 1;
        """.format(
            UUID = UNIQUE_IDENTIFIER
        )       

        fnames = plpy.execute(sql)[0]['feature_names']
        vif_dict = {}

        for f in range(len(fnames)):
            #1) Create a table for VIF computation
            indx = f+1
            sql = """
                drop table if exists vif_regress_{UUID} cascade;
                create temp table vif_regress_{UUID}
                as
                (
                    select 
                        id,
                        vifdemo.filter_array_at_index(features, {indx}) as features,
                        features[{indx}] as target
                    from vif_fselection_tbl_{UUID}
                ) distributed by (id);
            """.format(
                id_column = id_column,
                indx = indx,
                nfeat = len(fnames),
                UUID = UNIQUE_IDENTIFIER
            )
            plpy.execute(sql)

            #2) Compute VIF by regressing current variable against rest of the variables
            sql = """
                drop table if exists vif_linregr_mdl_{UUID} cascade;
                drop table if exists vif_linregr_mdl_{UUID}_summary cascade;
                select  madlib.linregr_train(
                    'vif_regress_{UUID}', 
                    'vif_linregr_mdl_{UUID}', 
                    'target', 
                    'features'
                );                    
            """.format(
                UUID = UNIQUE_IDENTIFIER
            )
            plpy.execute(sql)
            sql = """
                select 
                    r2
                from vif_linregr_mdl_{UUID};
            """.format(UUID = UNIQUE_IDENTIFIER)
            r2_k = plpy.execute(sql)[0]['r2']
            #Compute VIF
            if r2_k is None:
                continue
            vif_k = 1.0/(1.0-r2_k) if r2_k !=1 else 1e100
            vif_dict[fnames[f]]=vif_k

        #If the vif_dict is empty, something's wrong, probably all rows were discarded
        #in the regression due to missing values
        if(not vif_dict):
            multicollinearity_exists = False
            continue
        #Remove the feature with max VIF
        feat, max_vif = max(vif_dict.items(), key=itemgetter(1))
        sql = """
            insert into {vif_results_table} 
            values ({step},'{feat}',{vif},{discarded});
        """.format(
            vif_results_table = vif_results_table,
            step = step,
            feat = feat,
            vif = max_vif,
            discarded = 'True' if max_vif > vif_threshold else 'False'
        )
        plpy.execute(sql)        
        #Check if the max_vif is > vif_threshold
        if max_vif < vif_threshold:
            multicollinearity_exists = False
        else:
            #Remove the feature with max_vif and iterate again
            sql = """
                select
                    feature_names
                from
                    vif_fselection_tbl_{UUID}
                limit 1;
            """.format(
                UUID = UNIQUE_IDENTIFIER
            )

            #postgres array indices start from 1, so add 1 to the index.
            feat_indx = plpy.execute(sql)[0]['feature_names'].index(feat) + 1

            vif_fselect_table_sql = """
                drop table if exists vif_fselection_tbl_swap_{UUID} cascade;
                create temp table vif_fselection_tbl_swap_{UUID}
                as
                (
                    select 
                        id,
                        vifdemo.filter_array_at_index(feature_names, {feat_indx}) as feature_names,
                        vifdemo.filter_array_at_index(features, {feat_indx}) as features
                    from 
                        vif_fselection_tbl_{UUID}
                ) distributed by (id);
            """.format(
                feat_indx = feat_indx,
                UUID = UNIQUE_IDENTIFIER
            )
            plpy.execute(vif_fselect_table_sql)  

            #Rename swap table to original vif_table (as in-place mods are not allowed)
            sql = """
                drop table if exists vif_fselection_tbl_{UUID};
                alter table vif_fselection_tbl_swap_{UUID}
                rename to vif_fselection_tbl_{UUID};
            """.format(
                UUID = UNIQUE_IDENTIFIER
            )  
            plpy.execute(sql)    
        step += 1 

    sql = """
        drop table if exists vif_linregr_mdl_{UUID} cascade;
        drop table if exists vif_linregr_mdl_{UUID}_summary cascade;
    """.format(
        UUID = UNIQUE_IDENTIFIER
    )
    plpy.execute(sql)

    return """
        Results saved in {vif_results_table}
    """.format(
            vif_results_table = vif_results_table
    )
$$ language plpythonu;


-----------------------------------------------------------------------------------------------------
-- 2) Sample invocation
-----------------------------------------------------------------------------------------------------

--a) If your table is of the form ||id | <list of feature columns> | label_column | ||
select vifdemo.feature_selection_w_vif(
    'input_table', -- Input table
    'input_schema', -- input schema
    'id', -- ID Column
    'multi_class_label', -- Label column
    -- Exclude columns
    ARRAY[
        'id', 
        'multi_class_label',
        'year'
    ],
    10, -- ViF threshold (recommend >= 10)
    10, -- num_steps (max value is total number of indepdendent variables you have)
    'vifdemo.vif_results_table' -- Results table
);

--b) If your table is of the form ||id | feature_names text[] | feature_vals float8[]||
select vifdemo.feature_selection_w_vif(
    'input_schema.input_table', -- Input table
    'id', -- ID column
    'feat_name_vect', -- Features names (array) column
    'feat_vect_normalized', -- feature values (array) column
    10, -- ViF threshold (recommend >= 10),
    20, -- num_steps (max values is total number of independent variables you have). 
    'vifdemo.vif_results_table' -- Results table
);

-----------------------------------------------------------------------------------------------------
-- 
-----------------------------------------------------------------------------------------------------    