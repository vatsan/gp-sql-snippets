

------------------------------------------------------------------------------------------------------------------------------------
-- Run K-Means clustering with varying K
------------------------------------------------------------------------------------------------------------------------------------

-- 1) UDF to iterate over K
--PL/Python Driver Function to run K-Means while varying the number of clusters
drop function if exists YOUR_SCHEMA.run_kmeans(
	text, 
	text, 
	text, 
	text, 
	text, 
	text, 
	int[], 
	text
) cascade;
create or replace function YOUR_SCHEMA.run_kmeans(
	kmeans_algo_type text,
	source_table text,
	id_column text,
	feature_vect_column text,
	cluster_alloc_dist_metric text,
	centroid_dist_metric text,
	num_clusters_arr int[],
	mdl_output_tbl text
)
returns text
as
$$
	import plpy
	#1) Determine if we need K-Means with random initialization or kmeans++
	algo_type = 'kmeanspp' if kmeans_algo_type == 'kmeanspp' else 'kmeans_random'
	
	#2) Prepare a table to hold model results
	sql = '''
	--Prepare results table
	drop table if exists {mdl_output_tbl} cascade;
	create table {mdl_output_tbl} 
	(
		num_clusters int,
		centroids double precision[],
		objective_fn double precision,
		frac_reassigned double precision,
		num_iterations int,
		simple_silhouette double precision
	) distributed randomly;
	'''.format(mdl_output_tbl = mdl_output_tbl)
	results = plpy.execute(sql)
	
	#3) Run MADlib K-Means with supplied arguments for each value in the num_clusters_arr array
	for num_clusters in num_clusters_arr:
	sql = '''
		insert into {mdl_output_tbl}
		with mdl
		as
		(
			select 
				{num_clusters} as num_clusters,
				   *
			from 
				madlib.{kmeans_algo_type}(
					 '{source_table}', 
					 '{feature_vect_column}', 
					 {num_clusters}, 
					 '{cluster_alloc_dist_metric}', 
					 '{centroid_dist_metric}', 
					 60,-- num_iterations 
					 0.001 -- stoping criterion (fraction of points re-assigned)
				 )
		)
		select 
			mdl.*,
			scoef.simple_silhouette
		from
		(
			select 
				num_clusters,
				--Refer: http://en.wikipedia.org/wiki/Silhouette_(clustering)
				avg(
					CASE  WHEN distances[2] = 0 THEN 0
					  ELSE (distances[2] - distances[1]) / distances[2]
					END
				) as simple_silhouette
			from
			(
				select 
					ds.{id_column}, 
					mdl.num_clusters,
					( 
						madlib.closest_columns(
							 mdl.centroids, 
							 ds.{feature_vect_column}, 
							 2,
							 '{cluster_alloc_dist_metric}'
						)
					).distances
				from 
					{source_table} ds, mdl
			)q
			group by 
				num_clusters
		) scoef, mdl
		where scoef.num_clusters = mdl.num_clusters
	'''.format(
		kmeans_algo_type = kmeans_algo_type,
		source_table = source_table,
		feature_vect_column = feature_vect_column,
		num_clusters = num_clusters,
		cluster_alloc_dist_metric = cluster_alloc_dist_metric,
		centroid_dist_metric = centroid_dist_metric,
		mdl_output_tbl = mdl_output_tbl,
		id_column = id_column
	)
	plpy.execute(sql)
	
	return 'Model parameters written to {mdl_output_tbl} table'.format(mdl_output_tbl = mdl_output_tbl) 
$$ language plpythonu;


--2) Run K-Means with random centroid initialization, with varying number of clusters
select YOUR_SCHEMA.run_kmeans(
	'kmeans_random',
	'YOUR_SCHEMA.test_normalizer',
	'person_uid',
	'feat_vect_normalized',
	'madlib.squared_dist_norm2',
	'madlib.avg',
	-- Different cluster sizes.
	ARRAY[8, 12, 16, 20, 24, 28, 32, 36],
	'YOUR_SCHEMA.kmeans_random_mdl_params'
);

--3) Run K-Means++ with varying number of clusters
select YOUR_SCHEMA.run_kmeans(
	'kmeanspp',
	'YOUR_SCHEMA.test_normalizer',
	'person_uid',
	'feat_vect_normalized',
	'madlib.squared_dist_norm2',
	'madlib.avg',
	-- Different cluster sizes.
	ARRAY[8, 12, 16, 20, 24, 28, 32, 36],
	'YOUR_SCHEMA.kmeanspp_mdl_params'
);

--4) Pick best {algo type, num_clusters} combination based on simple_silhouette
drop table if exists YOUR_SCHEMA.kmeans_silhouette cascade;
create table YOUR_SCHEMA.kmeans_silhouette
as
(
	select 
		algo_type,
		num_clusters,
		objective_fn,
		frac_reassigned,
		num_iterations,
		simple_silhouette
	from
	(
		select 
			'kmeans_random' as algo_type,
			   *
		from 
			YOUR_SCHEMA.kmeans_random_mdl_params
		union all
		select 
			'kmeans++' as algo_type,
			   *
		from 
			YOUR_SCHEMA.kmeanspp_mdl_params
	) all_variants
) distributed randomly;

--5) Show optimal algo_type & num_clusters.
select *
from 
	YOUR_SCHEMA.kmeans_silhouette
order by 
	simple_silhouette desc;