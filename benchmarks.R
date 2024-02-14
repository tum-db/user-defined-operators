library(data.table)
library(jsonlite)
library(ggplot2)
library(scales)
library(tikzDevice)
library(RColorBrewer)

options(
    tikzDefaultEngine = "luatex",
    tikzDocumentDeclaration = "\\documentclass[12pt]{article}",
    tikzLualatexPackages = c(
      "\\usepackage{fontspec}",
      "\\usepackage{tikz}",
      "\\setmainfont{Libertinus Serif}",
      "\\usepackage[active,tightpage,psfixbb]{preview}",
      "\\PreviewEnvironment{pgfpicture}",
      "\\setlength\\PreviewBorder{0pt}"
    ),
    tikzUnicodeMetricPackages = "\\usetikzlibrary{calc}"
)


umbra_benchmarks_raw <- fread("benchmarks.log")

umbra_benchmarks_raw <- umbra_benchmarks_raw[, c("name", "compilation_time", "execution_times", "perf_counters", "db_state")]

json_to_columns <- function(json_strings) {
  json_strings2 <- gsub("\"\"", "\"", json_strings)
  return(rbindlist(lapply(json_strings2, fromJSON), use.names = TRUE, fill = TRUE))
}

perf_counters <- umbra_benchmarks_raw[, json_to_columns(perf_counters)]
db_state <- umbra_benchmarks_raw[, json_to_columns(db_state)]
db_state <- db_state[, c("compilationmode", "_dop", "wasm_disableboundschecks")]
setnames(db_state, "_dop", "dop")
db_state[, num_threads := as.integer(dop)]
db_state[, dop := NULL]

umbra_benchmarks_raw[, perf_counters := NULL]
umbra_benchmarks_raw[, db_state := NULL]

umbra_benchmarks <- as.data.table(c(umbra_benchmarks_raw, perf_counters, db_state))
umbra_benchmarks[, compilationmode := factor(compilationmode, levels = c("i", "a", "o"), labels = c("interpreted", "adaptive", "optimized"))]

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

contains <- function(v, pattern) {
  return(grepl(pattern=pattern, v, fixed=TRUE))
}

names <- umbra_benchmarks[, transpose(lapply(name, split_names))]
colnames(names) <- c("name", "data_size")
names <- names[, data_size := as.double(data_size)]
umbra_benchmarks[, name := NULL]
umbra_benchmarks <- as.data.table(c(names, umbra_benchmarks))

umbra_benchmarks <- melt(umbra_benchmarks, measure.vars = patterns("^V"), variable.name = "run_id", value.name = "runtime")
#umbra_benchmarks[wasm_disableboundschecks == "on", name := paste(name, "noboundschecks", sep="_")]
umbra_benchmarks[, runtime := as.double(runtime)]
umbra_benchmarks[, run_id := as.integer(substring(run_id, 2))]
umbra_benchmarks[, system := "Umbra"]

benchmarks_colnames <- c("system", "num_threads", "name", "data_size", "compilation_time", "runtime")
benchmarks <- umbra_benchmarks[, ..benchmarks_colnames]


standalone_benchmarks <- fread("standalone-benchmarks.log")
standalone_benchmarks[, runtime := as.double(execution_ns + csvparse_ns) / 1e9]
standalone_benchmarks[, data_size := as.double(num_tuples)]
standalone_benchmarks[, compilation_time := 0]
standalone_benchmarks[, execution_ns := NULL]
standalone_benchmarks[, csvparse_ns := NULL]
standalone_benchmarks[, num_tuples := NULL]
standalone_benchmarks[, system := "Standalone"]
standalone_benchmarks[, num_threads := 56]

benchmarks <- rbind(benchmarks, standalone_benchmarks[, ..benchmarks_colnames], fill=TRUE)


postgres_benchmarks <- fread("postgres-benchmarks.log")
postgres_benchmarks[, runtime := as.double(execution_ms) / 1000]
postgres_benchmarks[, data_size := as.double(num_tuples)]
postgres_benchmarks[, execution_ms := NULL]
postgres_benchmarks[, compilation_time := as.double(planning_ms) / 1000]
postgres_benchmarks[, planning_ms := NULL]
postgres_benchmarks[, num_tuples := NULL]
postgres_benchmarks[, system := "Postgres"]
postgres_benchmarks[, num_threads := 1]
setnames(postgres_benchmarks, "query", "name")

benchmarks <- rbind(benchmarks, postgres_benchmarks[, ..benchmarks_colnames], fill=TRUE)


duckdb_benchmarks <- fread("duckdb-benchmarks.log")
duckdb_benchmarks[, runtime := as.double(time_s)]
duckdb_benchmarks[, data_size := as.double(num_tuples)]
duckdb_benchmarks[, compilation_time := 0]
duckdb_benchmarks[, time_s := NULL]
duckdb_benchmarks[, num_tuples := NULL]
duckdb_benchmarks[, system := "DuckDB"]
duckdb_benchmarks[, num_threads := 56]
setnames(duckdb_benchmarks, "query", "name")

benchmarks <- rbind(benchmarks, duckdb_benchmarks[, ..benchmarks_colnames], fill=TRUE)


spark_benchmarks <- fread("spark-benchmarks.log")
spark_benchmarks[, runtime := as.double(time_in_ms) / 1000]
spark_benchmarks[, data_size := as.double(num_tuples)]
spark_benchmarks[, compilation_time := 0]
spark_benchmarks[, time_in_ms := NULL]
spark_benchmarks[, num_tuples := NULL]
spark_benchmarks[, system := "Spark"]
spark_benchmarks[, num_threads := 56]

benchmarks <- rbind(benchmarks, spark_benchmarks[, ..benchmarks_colnames], fill=TRUE)

benchmarks[, udo_type := as.character(NA)]
benchmarks[contains(name, "cxxudo"), udo_type := "cxxudo"]
benchmarks[contains(name, "wasmudo"), udo_type := "wasmudo"]
benchmarks[udo_type == "wasmudo" & contains(name, "full_boundschecks"), wasm_boundschecks := "full"]
benchmarks[udo_type == "wasmudo" & contains(name, "optimized_boundschecks"), wasm_boundschecks := "optimized"]
benchmarks[udo_type == "wasmudo" & contains(name, "no_boundschecks"), wasm_boundschecks := "no"]

benchmarks[, runtime_single_threaded := runtime * num_threads]
benchmarks[, runtime_with_compilation := runtime + compilation_time]
benchmarks[, runtime_with_compilation_single_threaded := runtime_single_threaded + compilation_time]
benchmarks[, implementation := paste(system, name)]


throughput_mean <- function(v) {
  return(sqrt(mean(v**2)))
}

geomean <- function(v) {
  return(exp(mean(log(v))))
}

M_labels <- function(value) {
  #return(paste(as.integer(value) / 1000000, "M", sep=""))
  return(as.double(value) / 1000000)
}

B_labels <- function(value) {
  return(as.double(value) / 1000000000)
}

Gi_labels <- function(value) {
  return(as.double(value) / 2**30)
}

brewer_palette <- function(n) {
  return(brewer.pal(n, "Set1"))
}

color_map <- setNames(
  brewer_palette(5)[c(3, 2, 1, 4, 5)],
  c("Umbra", "Postgres", "Spark", "Standalone", "DuckDB")
)
umbra_alt_color <- "#94af94"
shape_map <- setNames(
  c(4, 0, 1, 2, 5),
  c("Umbra", "Postgres", "Spark", "Standalone", "DuckDB")
)

color_map_udo_type <- setNames(
  color_map[c("Umbra", "Postgres")],
  c("C++ UDO", "WebAssembly UDO")
)
shape_map_udo_type <- setNames(
  c(0, 4),
  c("C++ UDO", "WebAssembly UDO")
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
    axis.title.x=element_text(margin=margin(t=6)),
    axis.title.y.left=element_text(margin=margin(r=10)),
    axis.title.y.right=element_text(margin=margin(l=10)),
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
shape_size <- 2.2
shape_stroke <- 1.2

udo_names <- c(
  "cxxudo",
  "wasmudo"
)
udo_labels <- c(
  "C++ UDO",
  "WebAssembly UDO"
)

kmeans_means <- benchmarks[
  contains(name, "kmeans") & !contains(name, "_threads"),
  .(
    runtime = throughput_mean(runtime),
    runtime_single_threaded = throughput_mean(runtime_single_threaded),
    runtime_with_compilation = throughput_mean(runtime_with_compilation),
    runtime_with_compilation_min = min(runtime_with_compilation),
    runtime_with_compilation_max = max(runtime_with_compilation),
    runtime_with_compilation_single_threaded = throughput_mean(runtime_with_compilation_single_threaded),
    compilation_time = throughput_mean(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "udo_type", "wasm_boundschecks")
]

kmeans_trans_func <- function(xs) {
  large_limit <- 10000000
  large_data <- xs > large_limit
  xs[large_data] <- large_limit + (xs[large_data] - large_limit) / 40
  return(xs)
}
kmeans_trans_inv <- function(xs) {
  large_limit <- 10000000
  large_data <- xs > large_limit
  xs[large_data] <- large_limit + (xs[large_data] - large_limit) * 40
  return(xs)
}
kmeans_trans <- trans_new("kmeans_trans", kmeans_trans_func, kmeans_trans_inv)

kmeans_plot_data <- kmeans_means[
  (data_size <= 10000000) & (is.na(udo_type) | udo_type != "wasmudo"),
]
kmeans_names <- c(
  "cxxudo_kmeans",
  "umbra_kmeans",
  "kmeans"
)
kmeans_labels <- c(
  "C++ UDO",
  "SQL / Spark",
  "SQL / Spark"
)
kmeans_plot_data <- kmeans_plot_data[, name := factor(name, kmeans_names, kmeans_labels)]
kmeans_plot_points <- kmeans_plot_data[data_size %% 1000000 == 0]
kmeans_plot <- (
  ggplot(kmeans_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=system, shape=system))
  + geom_line(aes(linetype=name), linewidth=line_size)
  #+ geom_line(aes(linetype=name, y=data_size / runtime_with_compilation_max), linewidth=1)
  #+ geom_line(aes(linetype=name, y=data_size / runtime_with_compilation_min), linewidth=1)
  + geom_point(data=kmeans_plot_points, size=shape_size, stroke=shape_stroke)
  #+ geom_errorbar(aes(ymin=data_size / runtime_with_compilation_max, ymax=data_size / runtime_with_compilation_min))
  + scale_y_continuous(labels=M_labels, limits=c(0, 95000000))
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (millions)", y = "throughput (M tuples / s)")
  + labs(x = NULL, y = NULL)
)


kmeans_plot_large_data <- kmeans_means[
  (data_size == 10000000 | data_size == 50000000 | data_size >= 100000000) & (system == "Umbra" | system == "Standalone") & (is.na(udo_type) | udo_type != "wasmudo"),
]
kmeans_plot_large_points <- kmeans_plot_large_data
kmeans_plot_large <- (
  ggplot(kmeans_plot_large_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=system, shape=system))
  + geom_line(aes(linetype=name), linewidth=line_size)
  + geom_point(data=kmeans_plot_large_points, size=shape_size, stroke=shape_stroke)
  + scale_y_continuous(labels=M_labels, limits=c(0, 95000000), position="right")
  + scale_x_continuous(labels=M_labels, breaks=c(10000000, seq(100000000, 500000000, 100000000)), limits=c(10000000, NA))
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (millions)", y = "throughput (M tuples / s)")
  + labs(x = NULL, y = NULL)
)


kmeans_wasm_plot_data <- kmeans_means[
  (system == "Umbra") & (data_size <= 20000000) & !is.na(udo_type)
]
kmeans_wasm_plot_data[udo_type == "cxxudo", wasm_boundschecks := "no"]
kmeans_wasm_plot_data[, udo_type := factor(udo_type, udo_names, udo_labels)]
kmeans_wasm_plot_points <- kmeans_wasm_plot_data[data_size %% 1000000 == 0]
kmeans_wasm_plot <- (
  ggplot(kmeans_wasm_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=udo_type, shape=udo_type, linetype=wasm_boundschecks))
  + geom_line(linewidth=line_size)
  + geom_point(data=kmeans_wasm_plot_points, size=shape_size, stroke=shape_stroke)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA))
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map_udo_type, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map_udo_type, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("dotdash", "solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of tuples (millions)", y = "throughput (M tuples / s)")
)


kmeans_threads_means <- benchmarks[
  (
    system == "Umbra" &
      contains(name, "kmeans") & contains(name, "_threads")
  ),
  .(
    runtime = throughput_mean(runtime),
    runtime_with_compilation = throughput_mean(runtime_with_compilation),
    compilation_time = throughput_mean(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "num_threads", "udo_type", "wasm_boundschecks")
]

kmeans_threads_data <- kmeans_threads_means[is.na(udo_type) | udo_type != "wasmudo"]
kmeans_threads_names <- c(
  "cxxudo_kmeans_threads",
  "umbra_kmeans_threads"
)
kmeans_threads_labels <- c(
  "C++ UDO",
  "Umbra native"
)
kmeans_threads_data[, name := factor(name, kmeans_threads_names, kmeans_threads_labels)]
kmeans_threads_points <- kmeans_threads_data[
  (num_threads <= 28 & (num_threads %% 4 == 0)) |
    (num_threads > 28 & (num_threads %% 8 == 0))
]

kmeans_threads_plot <- (
  ggplot(kmeans_threads_data, aes(x=num_threads))
  #+ geom_line(aes(y=data_size / runtime_with_compilation, linetype=name), color=umbra_alt_color, linewidth=line_size)
  #+ geom_point(aes(y=data_size / runtime_with_compilation), data=kmeans_threads_plot_points, color=color_map["Umbra"], shape=shape_map["Umbra"], size=shape_size, stroke=shape_stroke)
  + geom_line(aes(y=data_size / runtime, linetype=name), color=color_map["Umbra"], linewidth=line_size)
  + geom_point(aes(y=data_size / runtime), data=kmeans_threads_points, color=color_map["Umbra"], shape=shape_map["Umbra"], size=shape_size, stroke=shape_stroke)
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=25000000, label="SMT", size=3)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + coord_trans(x=threads_trans)
  + scale_linetype_manual(values=c("solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of threads", y = "throughput (M tuples / s)")
)


kmeans_threads_wasm_data <- kmeans_threads_means[!is.na(udo_type)]
kmeans_threads_wasm_data[udo_type == "cxxudo", wasm_boundschecks := "no"]
kmeans_threads_wasm_data[, udo_type := factor(udo_type, udo_names, udo_labels)]
kmeans_threads_wasm_points <- kmeans_threads_wasm_data[
  (num_threads <= 28 & (num_threads %% 4 == 0)) |
    (num_threads > 28 & (num_threads %% 8 == 0))
]

kmeans_threads_wasm_plot <- (
  ggplot(kmeans_threads_wasm_data, aes(x=num_threads, y=data_size/runtime, color=udo_type, shape=udo_type, linetype=wasm_boundschecks))
  + geom_line(linewidth=line_size)
  + geom_point(data=kmeans_threads_wasm_points, size=shape_size, stroke=shape_stroke)
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=25000000, label="SMT", size=3)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + coord_trans(x=threads_trans)
  + scale_color_manual(values=color_map_udo_type, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map_udo_type, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("dotdash", "solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of threads", y = "throughput (M tuples / s)")
)

regression_means <- benchmarks[
  contains(name, "regression") & !contains(name, "_threads"),
  .(
    runtime = throughput_mean(runtime),
    runtime_single_threaded = throughput_mean(runtime_single_threaded),
    runtime_with_compilation = throughput_mean(runtime_with_compilation),
    runtime_with_compilation_single_threaded = throughput_mean(runtime_with_compilation_single_threaded),
    compilation_time = throughput_mean(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "udo_type", "wasm_boundschecks")
]
regression_means[system == "Spark", name := "spark_regression"]
regression_means[system == "DuckDB", name := "sql_regression"]
regression_means_small <- regression_means[data_size <= 1000000000]
regression_means_large <- regression_means[data_size >= 1000000000]

regression_names <- c(
  "cxxudo_regression",
  "sql_regression",
  "umbra_regression",
  "spark_regression"
)
regression_labels <- c(
  "C++ UDO",
  "SQL",
  "Umbra / Spark native",
  "Umbra / Spark native"
)

regression_plot_data <- regression_means_small[is.na(udo_type) | udo_type != "wasmudo"]
regression_plot_data[, name := factor(name, regression_names, regression_labels)]
regression_plot <- (
  ggplot(regression_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=system, shape=system))
  + geom_line(aes(linetype=name), linewidth=line_size)
  + geom_point(data=regression_plot_data[data_size %% 1000000 == 0], size=shape_size, stroke=shape_stroke)
  + scale_y_continuous(labels=B_labels, limits=c(0, 5e9))#, sec.axis=sec_axis(~. * 16 / 2**30, name="throughput (GiB/s)"))
  + scale_x_continuous(labels=B_labels)
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("solid", "dotdash", "longdash"), labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (billions)", y = "throughput (B tuples / s)")
  + labs(x = NULL, y = NULL)
)

regression_plot_large_data <- regression_means_large[(system == "Umbra" | system == "DuckDB") & (is.na(udo_type) | udo_type != "wasmudo")]
regression_plot_large_data[, name := factor(name, regression_names, regression_labels)]
regression_plot_large_points <- regression_plot_large_data[data_size == 1e9 | (data_size %% 10e9 == 0)]
regression_plot_large <- (
  ggplot(regression_plot_large_data, aes(x=data_size, linetype=name, color=system, shape=system))
  + geom_line(aes(y=data_size / runtime_with_compilation), linewidth=line_size)
  + geom_point(aes(y=data_size / runtime_with_compilation), data=regression_plot_large_points, size=shape_size, stroke=shape_stroke)
  #+ geom_line(aes(y=data_size / runtime), color=color_map["Umbra"], linewidth=line_size)
  #+ geom_point(aes(y=data_size / runtime), color=color_map["Umbra"], shape=4, size=shape_size, stroke=shape_stroke)
  + scale_y_continuous(labels=NULL, limits=c(0, 5e9), sec.axis=sec_axis(~. * 16, labels=Gi_labels, breaks=seq(0, 64*2**30, 16*2**30)))
  + scale_x_continuous(labels=B_labels, breaks=c(1e9, seq(10e9, 40e9, 10e9)), limits=c(1e9, 40e9))
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("solid", "dotdash", "longdash"), labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (billions)", y = "throughput (B tuples / s)")
  + labs(x = NULL, y = NULL)
  + theme(axis.ticks.length.y.left = unit(0, "mm"))
)

regression_wasm_plot_data  <- regression_means[
  system == "Umbra" & !is.na(udo_type)
]
regression_wasm_plot_data[udo_type == "cxxudo", wasm_boundschecks := "no"]
regression_wasm_plot_data[, udo_type := factor(udo_type, udo_names, udo_labels)]
regression_wasm_plot_points <- regression_wasm_plot_data[(data_size == 100000000) | (data_size == 2000000000) | (data_size %% 4000000000 == 0)]
regression_wasm_plot <- (
  ggplot(regression_wasm_plot_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=udo_type, shape=udo_type, linetype=wasm_boundschecks))
  + geom_line(linewidth=line_size)
  + geom_point(data=regression_wasm_plot_points, size=shape_size, stroke=shape_stroke)
  + scale_y_continuous(labels=B_labels, limits=c(0, NA), sec.axis=sec_axis(~. * 16, labels=Gi_labels, breaks=seq(0, 64*2**30, 16*2**30), name="throughput (GiB/s)"))
  + scale_x_continuous(labels=B_labels)
  + scale_color_manual(values=color_map_udo_type, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map_udo_type, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("dotdash", "solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of tuples (billions)", y = "throughput (B tuples / s)")
)

regression_threads_means <- benchmarks[
  (
    system == "Umbra" &
    contains(name, "regression") & contains(name, "_threads")
  ),
  .(
    runtime = throughput_mean(runtime),
    runtime_with_compilation = throughput_mean(runtime_with_compilation),
    compilation_time = throughput_mean(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "num_threads", "udo_type", "wasm_boundschecks")
]

regression_threads_data <- regression_threads_means[is.na(udo_type) | udo_type != "wasmudo"]
regression_threads_names <- c(
  "cxxudo_regression_threads",
  "umbra_regression_threads",
  "sql_regression_threads"
)
regression_threads_labels <- c(
  "C++ UDO",
  "Umbra native",
  "SQL"
)
regression_threads_data[, name := factor(name, regression_threads_names, regression_threads_labels)]
regression_threads_points <- regression_threads_data[
  (num_threads <= 28 & (num_threads %% 4 == 0)) |
    (num_threads > 28 & (num_threads %% 8 == 0))
]

regression_threads_plot <- (
  ggplot(regression_threads_data, aes(x=num_threads, shape=name))
  + geom_line(aes(linetype=name, y=data_size / runtime), color=umbra_alt_color, linewidth=line_size)
  #+ geom_point(data=regression_threads_points[!contains(name, "Wasm")], color=umbra_alt_color, shape=shape_map["Umbra"], size=shape_size, stroke=shape_stroke)
  + geom_line(aes(linetype=name, y=data_size / runtime_with_compilation), color=color_map["Umbra"], linewidth=line_size)
  #+ geom_point(data=regression_threads_points[!contains(name, "Wasm")], aes(y=data_size / runtime), color=color_map["Umbra"], shape=shape_map["Umbra"], size=shape_size, stroke=shape_stroke)
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=1000000000, label="SMT", size=3)
  + scale_y_continuous(labels=B_labels, limits=c(0, NA), sec.axis=sec_axis(~. * 16, name="throughput (GiB/s)", labels=Gi_labels, breaks=seq(0, 64*2**30, 16*2**30)))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + coord_trans(x=threads_trans)
  + scale_linetype_manual(values=c("solid", "longdash", "dotdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of threads", y = "throughput (B tuples / s)")
)


regression_threads_wasm_data <- regression_threads_means[!is.na(udo_type)]
regression_threads_wasm_data[udo_type == "cxxudo", wasm_boundschecks := "no"]
regression_threads_wasm_data[, udo_type := factor(udo_type, udo_names, udo_labels)]
regression_threads_wasm_points <- regression_threads_wasm_data[
  (num_threads <= 28 & (num_threads %% 4 == 0)) |
    (num_threads > 28 & (num_threads %% 8 == 0))
]

regression_threads_wasm_plot <- (
  ggplot(regression_threads_wasm_data, aes(x=num_threads, y=data_size/runtime_with_compilation, color=udo_type, shape=udo_type, linetype=wasm_boundschecks))
  + geom_line(linewidth=line_size)
  + geom_point(data=regression_threads_wasm_points, size=shape_size, stroke=shape_stroke)
  + geom_vline(xintercept=28)
  + annotate("rect", xmin=28, xmax=Inf, ymin=-Inf, ymax=Inf, color="grey50", alpha=0.1)
  + annotate("text", x=44, y=25000000, label="SMT", size=3)
  + scale_y_continuous(labels=B_labels, limits=c(0, NA), sec.axis=sec_axis(~. * 16, name="throughput (GiB/s)", labels=Gi_labels, breaks=seq(0, 64*2**30, 16*2**30)))
  + scale_x_continuous(breaks=c(8, 16, 24, 32, 40, 48, 56))
  + coord_trans(x=threads_trans)
  + scale_color_manual(values=color_map_udo_type, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map_udo_type, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("dotdash", "solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of threads", y = "throughput (B tuples / s)")
)


words_medians <- benchmarks[
  grepl(pattern="words", name, fixed = TRUE),
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

arrays_means <- benchmarks[
  contains(name, "arrays"),
  .(
    runtime = throughput_mean(runtime),
    runtime_single_threaded = throughput_mean(runtime_single_threaded),
    runtime_with_compilation = throughput_mean(runtime_with_compilation),
    runtime_with_compilation_single_threaded = throughput_mean(runtime_with_compilation_single_threaded),
    compilation_time = throughput_mean(compilation_time)
  ),
  by = c("system", "name", "implementation", "data_size", "udo_type", "wasm_boundschecks")
]

arrays_plot_data <- arrays_means[is.na(udo_type) | udo_type != "wasmudo"]
arrays_plot_data[name == "postgres_arrays", name := "arrays_unnest"]
arrays_plot_data[,
  name := factor(
    name,
    c("recursive_sql_arrays", "arrays_unnest", "cxxudo_arrays"),
    c("Rec. CTE", "SQL unnest", "UDO")
  )
]

arrays_plot_small_data <- arrays_plot_data[data_size <= 1000000]
arrays_small_plot <- (
  ggplot(arrays_plot_small_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=system, shape=system, linetype=name))
  + geom_line(linewidth=line_size)
  + geom_point(data=arrays_plot_small_data[data_size %% 100000 == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA))
  + scale_x_continuous(labels=M_labels)
  + scale_linetype_manual(values=c("dotted", "longdash", "solid"), labels=NULL, guide="none")
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (millions)", y = "throughput (M tuples/s)")
  + labs(x = NULL, y = NULL)
)

arrays_plot_large_data <- arrays_plot_data[data_size >= 1000000]
arrays_large_plot <- (
  ggplot(arrays_plot_large_data, aes(x=data_size, y=data_size / runtime_with_compilation, color=system, shape=system, linetype=name))
  + geom_line(linewidth=line_size)
  + geom_point(data=arrays_plot_large_data[(data_size == 1000000) | (data_size %% 2000000) == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA), position="right")
  + scale_x_continuous(labels=M_labels, breaks=c(1000000, seq(2000000, 10000000, 2000000)), limits=c(1000000, NA))
  + scale_linetype_manual(values=c("dotted", "longdash", "solid"), labels=NULL, guide="none")
  + scale_color_manual(values=color_map, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map, labels=NULL, guide="none")
  + mytheme
  #+ labs(x = "number of tuples (millions)", y = "throughput (M tuples/s)")
  + labs(x = NULL, y = NULL)
)

arrays_wasm_plot_data <- arrays_means[(system == "Umbra") & !is.na(udo_type)]
arrays_wasm_plot_data[udo_type == "cxxudo", wasm_boundschecks := "no"]
arrays_wasm_plot_data[, udo_type := factor(udo_type, udo_names, udo_labels)]
arrays_wasm_plot <- (
  ggplot(arrays_wasm_plot_data, aes(x=data_size, y=data_size / runtime, color=udo_type, shape=udo_type, linetype=wasm_boundschecks))
  + geom_line(linewidth=line_size)
  + geom_point(data=arrays_wasm_plot_data[data_size %% 1000000 == 0], size=shape_size)
  + scale_y_continuous(labels=M_labels, limits=c(0, NA))
  + scale_x_continuous(labels=M_labels)
  + scale_color_manual(values=color_map_udo_type, labels=NULL, guide="none")
  + scale_shape_manual(values=shape_map_udo_type, labels=NULL, guide="none")
  + scale_linetype_manual(values=c("dotdash", "solid", "longdash"), labels=NULL, guide="none")
  + mytheme
  + labs(x = "number of tuples (millions)", y = "throughput (M tuples/s)")
)



ggsave("kmeans-throughput.tex", kmeans_plot, width=7, height=6, units='cm', device=tikz)
ggsave("kmeans-throughput-large.tex", kmeans_plot_large, width=5, height=6, units='cm', device=tikz)
ggsave("kmeans-threads.tex", kmeans_threads_plot, width=13, height=5, units='cm', device=tikz)
ggsave("kmeans-throughput-wasm.tex", kmeans_wasm_plot, width=13, height=6, units='cm', device=tikz)
ggsave("kmeans-threads-wasm.tex", kmeans_threads_wasm_plot, width=13, height=5, units='cm', device=tikz)
ggsave("regression-throughput.tex", regression_plot, width=7, height=6, units='cm', device=tikz)
ggsave("regression-throughput-large.tex", regression_plot_large, width=5, height=6, units='cm', device=tikz)
ggsave("regression-threads.tex", regression_threads_plot, width=13, height=5, units='cm', device=tikz)
ggsave("regression-throughput-wasm.tex", regression_wasm_plot, width=13, height=6, units='cm', device=tikz)
ggsave("regression-threads-wasm.tex", regression_threads_wasm_plot, width=13, height=5, units='cm', device=tikz)
ggsave("arrays-throughput.tex", arrays_small_plot, width=7, height=6, units='cm', device=tikz)
ggsave("arrays-throughput-large.tex", arrays_large_plot, width=5, height=6, units='cm', device=tikz)
ggsave("arrays-throughput-wasm.tex", arrays_wasm_plot, width=13, height=6, units='cm', device=tikz)
