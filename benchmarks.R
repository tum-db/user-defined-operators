library(data.table)
library(jsonlite)
library(ggplot2)
library(scales)
library(tikzDevice)
library(RColorBrewer)

options(tikzDefaultEngine = "luatex")

umbra_benchmarks_raw <- fread("benchmarks.log")

umbra_benchmarks_raw <- umbra_benchmarks_raw[, c("name", "compilation_time", "execution_times", "db_state")]

umbra_benchmarks_raw[, db_state1 := gsub("\"\"", "\"", db_state)]
json_values <- umbra_benchmarks_raw[, rbindlist(lapply(db_state1, fromJSON), use.names = TRUE, fill = TRUE)]
json_values <- json_values[, c("compilationmode", "_dop")]
setnames(json_values, "_dop", "dop")
json_values[, num_threads := as.integer(dop)]
json_values[, dop := NULL]

umbra_benchmarks_raw[, db_state := NULL]
umbra_benchmarks_raw[, db_state1 := NULL]

umbra_benchmarks <- as.data.table(c(umbra_benchmarks_raw, json_values))
umbra_benchmarks[,compilationmode := factor(compilationmode, levels = c("i", "a", "o"), labels = c("interpreted", "adaptive", "optimized"))]

umbra_benchmarks[, execution_times2 := gsub("\\[|\\]", "", execution_times)]
execution_times <- umbra_benchmarks[, tstrsplit(execution_times2, ",")]
umbra_benchmarks[, execution_times := NULL]
umbra_benchmarks[, execution_times2 := NULL]

umbra_benchmarks <- as.data.table(c(umbra_benchmarks, execution_times))

split_names <- function(n) {
  matches <- gregexpr("_", n, fixed = TRUE, useBytes = TRUE)[[1]]
  pos <- matches[length(matches)]
  return(c(substring(n, 0, pos - 1), substring(n, pos + 1)))
}

names <- umbra_benchmarks[, transpose(lapply(name, split_names))]
colnames(names) <- c("name", "data_size")
names <- names[, data_size := as.integer(data_size)]
umbra_benchmarks[, name := NULL]
umbra_benchmarks <- as.data.table(c(names, umbra_benchmarks))

umbra_benchmarks <- melt(umbra_benchmarks, measure.vars = patterns("^V"), variable.name = "run_id", value.name = "runtime")
umbra_benchmarks[, runtime := as.numeric(runtime)]
umbra_benchmarks[, run_id := as.integer(substring(run_id, 2))]
umbra_benchmarks[, system := "Umbra"]

benchmarks_colnames <- c("system", "num_threads", "name", "data_size", "compilation_time", "runtime")
benchmarks <- umbra_benchmarks[, ..benchmarks_colnames]


standalone_benchmarks <- fread("standalone-benchmarks.log")
standalone_benchmarks[, runtime := time_ns / 1e9]
standalone_benchmarks[, compilation_time := 0]
standalone_benchmarks[, time_ns := NULL]
standalone_benchmarks[, system := "Standalone"]
standalone_benchmarks[, num_threads := 56]
setnames(standalone_benchmarks, "num_tuples", "data_size")

benchmarks <- rbind(benchmarks, standalone_benchmarks[, ..benchmarks_colnames], fill=TRUE)


postgres_benchmarks <- fread("postgres-benchmarks.log")
postgres_benchmarks[, runtime := execution_ms / 1000]
postgres_benchmarks[, execution_ms := NULL]
postgres_benchmarks[, compilation_time := planning_ms / 1000]
postgres_benchmarks[, planning_ms := NULL]
postgres_benchmarks[, system := "Postgres"]
postgres_benchmarks[, num_threads := 1]
setnames(postgres_benchmarks, "query", "name")
setnames(postgres_benchmarks, "num_tuples", "data_size")

benchmarks <- rbind(benchmarks, postgres_benchmarks[, ..benchmarks_colnames], fill=TRUE)


duckdb_benchmarks <- fread("duckdb-benchmarks.log")
duckdb_benchmarks[, runtime := time_s]
duckdb_benchmarks[, compilation_time := 0]
duckdb_benchmarks[, time_s := NULL]
duckdb_benchmarks[, system := "DuckDB"]
duckdb_benchmarks[, num_threads := 56]
setnames(duckdb_benchmarks, "query", "name")
setnames(duckdb_benchmarks, "num_tuples", "data_size")

benchmarks <- rbind(benchmarks, duckdb_benchmarks[, ..benchmarks_colnames], fill=TRUE)


spark_benchmarks <- fread("spark-benchmarks.log")
spark_benchmarks[, runtime := time_in_ms / 1000]
spark_benchmarks[, compilation_time := 0]
spark_benchmarks[, time_in_ms := NULL]
spark_benchmarks[, system := "Spark"]
spark_benchmarks[, num_threads := 56]
setnames(spark_benchmarks, "num_tuples", "data_size")

benchmarks <- rbind(benchmarks, spark_benchmarks[, ..benchmarks_colnames], fill=TRUE)


benchmarks[, runtime_single_threaded := runtime * num_threads]
benchmarks[, runtime_with_compilation := runtime + compilation_time]
benchmarks[, runtime_with_compilation_single_threaded := runtime_single_threaded + compilation_time]
benchmarks[, implementation := paste(system, name)]


M_labels <- function(value) {
  #return(paste(as.integer(value) / 1000000, "M", sep=""))
  return(as.double(value) / 1000000)
}

brewer_palette <- function(n) {
  return(brewer.pal(n, "Set1"))
}

color_map <- setNames(
  brewer_palette(5)[c(3, 2, 1, 4, 5)],
  c("Umbra", "Postgres", "Spark", "Standalone", "DuckDB")
)
shape_map <- setNames(
  c(4, 0, 1, 2, 5),
  c("Umbra", "Postgres", "Spark", "Standalone", "DuckDB")
)

threads_trans_func <- function(xs) {
  ht_threads <- xs > 28
  xs[ht_threads] <- 28 + (xs[ht_threads] - 28) / 2
  return(xs)
}
threads_trans_inv <- function(xs) {
  ht_threads <- xs > 28
  xs[ht_threads] <- 28 + (xs[ht_threads] - 28) * 2
  return(xs)
}
threads_trans <- trans_new("threads_trans", threads_trans_func, threads_trans_inv)


mytheme <- (
  theme_bw()
  + theme(
    axis.title=element_text(size=8),
    legend.direction="vertical",
    legend.position=c(0.8,0.4),
    legend.background=element_rect(fill='transparent'),
    legend.title=element_blank(),
    legend.text=element_text(size=8),
    legend.key.height=unit(7, "pt"),
    legend.spacing=unit(0, "mm"),
    legend.margin=margin(0, 0, 0, 0)
  )
)
line_size <- 1.2
shape_size <- 1.8

kmeans_medians <- benchmarks[
  (name == "kmeans") | (name == "udo_kmeans"),
  .(
    runtime = median(runtime),
    runtime_single_threaded = median(runtime_single_threaded),
    runtime_with_compilation = median(runtime_with_compilation),
    runtime_with_compilation_single_threaded = median(runtime_with_compilation_single_threaded),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size")
]
kmeans_medians <- kmeans_medians[, name := factor(name, c("kmeans", "udo_kmeans"), c("Native", "UDO"))]
kmeans_plot_data <- kmeans_medians[
    data_size <= 10000000,
]
kmeans_plot <- (
  ggplot(kmeans_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation_single_threaded, color=system, shape=system))
  + geom_line(aes(linetype=name), size=line_size)
  + geom_point(data=kmeans_plot_data[data_size %% 1000000 == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels)
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map, labels=NULL, guide=FALSE)
  + scale_shape_manual(values=shape_map, labels=NULL, guide=FALSE)
  + scale_linetype_manual(values=c("longdash", "solid"), labels=NULL, guide=FALSE)
  + mytheme
  + theme(
      legend.position=c(0.58,0.4),
      legend.box="horizontal",
    )
  + labs(x = "number of tuples (millions)", y = NULL)
)

kmeans_threads_data <- benchmarks[
  (
    system == "Umbra" &
    ((name == "kmeans_threads") | (name == "udo_kmeans_threads")) &
    data_size == 500000000
  ),
  .(
    runtime = median(runtime),
    runtime_with_compilation = median(runtime_with_compilation),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "num_threads")
]
kmeans_threads_data[, implementation := factor(implementation, c("Umbra kmeans_threads", "Umbra udo_kmeans_threads"), c("Umbra native", "Umbra UDO"))]
kmeans_threads_points <- kmeans_threads_data[
  (num_threads <= 28 & (num_threads %% 4 == 0)) |
  (num_threads > 28 & (num_threads %% 8 == 0))
]
kmeans_threads_plot <- (
  ggplot(kmeans_threads_data, aes(x=num_threads, y=data_size / runtime_with_compilation))
  + geom_line(aes(linetype=implementation), color=color_map["Umbra"], size=line_size)
  + geom_point(data=kmeans_threads_points, color=color_map["Umbra"], shape=shape_map["Umbra"])
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=25000000, label="SMT", size=3)
  + scale_y_continuous(labels=M_labels, limits=c(0, 85000000))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + coord_trans(x=threads_trans)
  + scale_linetype_manual(values=c("longdash", "solid"), labels=NULL, guide=FALSE)
  + mytheme
  + theme(
      legend.position=c(0.75, 0.3),
    )
  + labs(x = "number of threads", y = NULL)
)


regression_medians <- benchmarks[
  (name == "regression") | (name == "udo_regression") | (name == "regression_2"),
  .(
    runtime = median(runtime),
    runtime_single_threaded = median(runtime_single_threaded),
    runtime_with_compilation = median(runtime_with_compilation),
    runtime_with_compilation_single_threaded = median(runtime_with_compilation_single_threaded),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size")
]
regression_medians[system == "Spark" | name == "regression", name := "native"]
regression_medians[,
  name := factor(
    name,
    c("native", "udo_regression", "regression_2"),
    c("SQL / Spark", "UDO", "Umbra native")
  )
]
regression_plot_data <- regression_medians
regression_plot <- (
  ggplot(regression_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation_single_threaded, color=system, shape=system))
  + geom_line(aes(linetype=name), size=line_size)
  + geom_point(size=shape_size)
  + scale_y_continuous(labels=M_labels)
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map, labels=NULL, guide=FALSE)
  + scale_shape_manual(values=shape_map, labels=NULL, guide=FALSE)
  + scale_linetype_manual(values=c("dotted", "solid", "longdash"), labels=NULL, guide=FALSE)
  + mytheme
  #+ theme(legend.position=c(0.55,0.55), legend.box="horizontal")
  + labs(x = "number of tuples (millions)", y = NULL)
)

regression_threads_data <- benchmarks[
  (
    system == "Umbra" &
      ((name == "regression_threads") | (name == "udo_regression_threads") | (name == "regression_2_threads")) &
      data_size == 1000000000
  )
]
regression_threads_medians <- regression_threads_data[,
  .(
    runtime = median(runtime),
    runtime_with_compilation = median(runtime_with_compilation),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "num_threads")
]
regression_threads_medians[,
  name := factor(
    name,
    c("regression_threads", "regression_2_threads", "udo_regression_threads"),
    c("regr\\_slope/intercept", "Native", "UDO")
  )
]
regression_threads_plot <- (
  ggplot(regression_threads_medians, aes(x=num_threads, y=data_size / runtime_with_compilation, shape=name))
  + geom_line(aes(linetype=name), color=color_map["Umbra"], size=line_size)
  + geom_line(aes(linetype=name, y=data_size / runtime), color="#94af94", size=line_size)
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=1000000000, label="SMT", size=3)
  + scale_y_continuous(labels=M_labels, limits=c(0, 6000000000), sec.axis=sec_axis(~. * 16 / 2**30, name="throughput (GiB/s)"))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + scale_linetype_manual(values=setNames(c("solid", "dotted", "longdash"), c("UDO", "regr\\_slope/intercept", "Native")), labels=NULL, guide=FALSE)
  + coord_trans(x=threads_trans)
  + mytheme
  + theme(legend.position=c(0.7, 0.3))
  + labs(x = "number of threads", y = NULL)
)


words_medians <- benchmarks[
  (name == "words") | (name == "udo_words"),
  .(
    runtime = median(runtime),
    runtime_single_threaded = median(runtime_single_threaded),
    runtime_with_compilation = median(runtime_with_compilation),
    runtime_with_compilation_single_threaded = median(runtime_with_compilation_single_threaded),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size")
]
words_medians[, name := factor(name, c("udo_words", "words"), c("UDO", "SQL ILIKE"))]
words_plot_with_compilation <- (
  ggplot(words_medians, aes(x=data_size, y=data_size / runtime_with_compilation_single_threaded, color=system, shape=system))
  + geom_line(aes(linetype=name), size=line_size)
  + geom_point(data=words_medians[data_size %% 10000000 == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels)
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map, labels=NULL, guide=FALSE)
  + scale_shape_manual(values=shape_map, labels=NULL, guide=FALSE)
  + mytheme
  + theme(legend.position=c(0.7,0.6))
  + labs(x = "number of tuples (millions)", y = "throughput (M tuples/s)")
)

arrays_medians <- benchmarks[
  (
    (name == "udo_arrays") |
    (name == "arrays_recursive") |
    (name == "arrays_unnest")
  ),
  .(
    runtime = median(runtime),
    runtime_single_threaded = median(runtime_single_threaded),
    runtime_with_compilation = median(runtime_with_compilation),
    runtime_with_compilation_single_threaded = median(runtime_with_compilation_single_threaded),
    compilation_time = median(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size")
]
arrays_plot_data <- arrays_medians[data_size <= 1000000]
arrays_plot_data[,
  name := factor(
    name,
    c("arrays_recursive", "arrays_unnest", "udo_arrays"),
    c("Rec. CTE", "SQL unnest", "UDO")
  )
]
arrays_plot_with_compilation <- (
  ggplot(arrays_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation_single_threaded, color=system, shape=system))
  + geom_line(aes(linetype=name), size=line_size)
  + geom_point(data=arrays_plot_data[data_size %% 100000 == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels)
  + scale_x_continuous(labels=M_labels)
  + scale_linetype_manual(values=c("dotted", "longdash", "solid"), labels=NULL, guide=FALSE)
  + scale_color_manual(values=color_map, labels=NULL, guide=FALSE)
  + scale_shape_manual(values=shape_map, labels=NULL, guide=FALSE)
  + mytheme
  + theme(legend.position=c(0.3,0.8))
  + labs(x = "number of tuples (millions)", y = NULL)
)


# In the paper we use the tex files
#ggsave("kmeans-throughput.tex", kmeans_plot, width=5, height=3, units='cm', device=tikz)
#ggsave("kmeans-threads.tex", kmeans_threads_plot, width=5, height=3, units='cm', device=tikz)
#ggsave("regression-throughput.tex", regression_plot, width=5, height=3, units='cm', device=tikz)
#ggsave("regression-threads.tex", regression_threads_plot, width=7, height=3, units='cm', device=tikz)
#ggsave("arrays-throughput.tex", arrays_plot_with_compilation, width=6, height=3, units='cm', device=tikz)
ggsave("kmeans-throughput.pdf", kmeans_plot, width=5, height=3, units='cm')
ggsave("kmeans-threads.pdf", kmeans_threads_plot, width=5, height=3, units='cm')
ggsave("regression-throughput.pdf", regression_plot, width=5, height=3, units='cm')
ggsave("regression-threads.pdf", regression_threads_plot, width=7, height=3, units='cm')
ggsave("arrays-throughput.pdf", arrays_plot_with_compilation, width=6, height=3, units='cm')
