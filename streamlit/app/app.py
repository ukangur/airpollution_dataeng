import streamlit as st
import duckdb
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import numpy as np

st.title("Timelines of overall features")

con = duckdb.connect("/app/data/airpolandweather.db", read_only=True)

# ---- Filters ----
st.sidebar.header("Filters")

# Filter by Year
query_years = "SELECT DISTINCT year FROM Date ORDER BY year;"
years = [row[0] for row in con.execute(query_years).fetchall()]
selected_years = st.sidebar.multiselect("Select Year(s)", years, default=years)

# Filter by Temperature
min_temp, max_temp = -40, 40
selected_temp = st.sidebar.slider(
    "Filter by Temperature (°C)", min_value=min_temp, max_value=max_temp, value=(min_temp, max_temp)
)

# Filter by Wind Speed
min_wind, max_wind = 0, 50
selected_wind_speed = st.sidebar.slider(
    "Filter by Wind Speed (m/s)", min_value=min_wind, max_value=max_wind, value=(min_wind, max_wind)
)

# Filter by Relative Humidity
min_humidity, max_humidity = 0, 100
selected_humidity = st.sidebar.slider(
    "Filter by Relative Humidity (%)", min_value=min_humidity, max_value=max_humidity, value=(min_humidity, max_humidity)
)

# Filter by Sea Level Pressure
min_pressure, max_pressure = 950, 1050
selected_pressure = st.sidebar.slider(
    "Filter by Sea Level Pressure (hPa)", min_value=min_pressure, max_value=max_pressure, value=(min_pressure, max_pressure)
)

if not selected_years:
    st.write("Please select at least one year to display results.")
else:
    query = f"""
    SELECT 
        d.year, d.month,
        AVG(o.air_temperature_c) AS avg_air_temperature_c,
        AVG(o.avg_wind_speed_m_s) AS avg_avg_wind_speed_m_s,
        AVG(o.relative_humidity_pct) AS avg_relative_humidity_pct,
        AVG(o.sea_level_pressure_hPa) AS avg_sea_level_pressure_hPa,
        AVG(o.so2_ug_m3) AS avg_so2,
        AVG(o.no2_ug_m3) AS avg_no2,
        AVG(o.pm25_ug_m3) AS avg_pm25,
        AVG(o.pm10_ug_m3) AS avg_pm10,
        AVG(o.o3_ug_m3) AS avg_o3
    FROM Observation o
    JOIN Date d ON o.date_id = d.id
    WHERE d.year IN ({','.join(map(str, selected_years))})
    AND o.air_temperature_c BETWEEN {selected_temp[0]} AND {selected_temp[1]}
    AND o.avg_wind_speed_m_s BETWEEN {selected_wind_speed[0]} AND {selected_wind_speed[1]}
    AND o.relative_humidity_pct BETWEEN {selected_humidity[0]} AND {selected_humidity[1]}
    AND o.sea_level_pressure_hPa BETWEEN {selected_pressure[0]} AND {selected_pressure[1]}
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """
    data = con.execute(query).fetchall()

    if data:
        dates = [datetime(int(row[0]), int(row[1]), 1) for row in data]
        temperatures = [row[2] for row in data]
        wind_speeds = [row[3] for row in data]
        humidities = [row[4] for row in data]
        pressures = [row[5] for row in data]
        so2 = [row[6] for row in data]
        no2 = [row[7] for row in data]
        pm25 = [row[8] for row in data]
        pm10 = [row[9] for row in data]
        o3 = [row[10] for row in data]

        fig1, axs1 = plt.subplots(2, 2, figsize=(26, 18), facecolor="none")
        fig1.patch.set_alpha(0.0)

        weather_data = [
            (temperatures, "Avg Temperature (°C)", "red"),
            (wind_speeds, "Avg Wind Speed (m/s)", "darkblue"),
            (humidities, "Avg Relative Humidity (%)", "teal"),
            (pressures, "Avg Sea Level Pressure (hPa)", "gold"),
        ]

        for i, (data, title, color) in enumerate(weather_data):
            row, col = divmod(i, 2)
            axs1[row, col].plot(dates, data, label=title, color=color, linewidth=2)
            axs1[row, col].set_title(title, color="white")
            axs1[row, col].set_xlabel("Date", color="white")
            axs1[row, col].set_ylabel("Value", color="white")
            axs1[row, col].tick_params(colors="white")
            axs1[row, col].legend(facecolor="none", edgecolor="none", labelcolor="white")
            axs1[row, col].set_facecolor("none")

        st.subheader(f"Plot 1: Weather metrics timelines")
        st.pyplot(fig1)

        fig2, axs2 = plt.subplots(3, 2, figsize=(26, 18), facecolor="none")
        fig2.patch.set_alpha(0.0)

        air_pollution_data = [
            (so2, "SO2 (µg/m³)", "blue"),
            (no2, "NO2 (µg/m³)", "green"),
            (pm25, "PM2.5 (µg/m³)", "purple"),
            (pm10, "PM10 (µg/m³)", "orange"),
            (o3, "Ozone (µg/m³)", "cyan"),
        ]

        for i, (data, title, color) in enumerate(air_pollution_data):
            row, col = divmod(i, 2)
            axs2[row, col].plot(dates, data, label=title, color=color, linewidth=2)
            axs2[row, col].set_title(title, color="white")
            axs2[row, col].set_xlabel("Date", color="white")
            axs2[row, col].set_ylabel("Concentration", color="white")
            axs2[row, col].tick_params(colors="white")
            axs2[row, col].legend(facecolor="none", edgecolor="none", labelcolor="white")
            axs2[row, col].set_facecolor("none")

        axs2[2, 1].axis("off")
        axs2[2, 1].set_facecolor("none")

        st.subheader(f"Plot 2: Airpollution metrics timelines")
        st.pyplot(fig2)

    st.title("Q1: Does temperature influence air pollution levels in Tallinn, and if so, how?")

    query_air_pollution = f"""
    SELECT 
        d.year, d.month, d.day,
        o.air_temperature_c, 
        o.max_temperature_c,
        o.min_temperature_c,
        o.so2_ug_m3, o.no2_ug_m3, o.pm25_ug_m3, o.pm10_ug_m3, o.o3_ug_m3
    FROM Observation AS o
    JOIN Date AS d ON o.date_id = d.id
    WHERE d.year IN ({','.join(map(str, selected_years))})
    AND o.air_temperature_c BETWEEN {selected_temp[0]} AND {selected_temp[1]}
    ORDER BY d.year, d.month, d.day;
    """

    data_air_pollution = con.execute(query_air_pollution).fetchall()

    if data_air_pollution:
        df = pd.DataFrame(data_air_pollution, columns=["year", "month", "day", "air_temperature_c", "max_temperature_c", "min_temperature_c", "so2", "no2", "pm25", "pm10", "o3"])
        df["date"] = pd.to_datetime(df[["year", "month", "day"]])
        df = df.drop(columns=["year", "month", "day"])

        # Plot 1: Scatter Plot of Temperature vs Air Pollution Metrics
        st.subheader(f"Plot 1: Scatter Plot of Temperature vs Air Pollution Metrics")
        air_pollution_metrics = [
            ("so2", "blue"),
            ("no2", "green"),
            ("pm25", "purple"),
            ("pm10", "orange"),
            ("o3", "cyan"),
        ]

        fig, axs = plt.subplots(3, 2, figsize=(26, 18), facecolor="none")
        fig.patch.set_alpha(0.0)

        for i, (metric, color) in enumerate(air_pollution_metrics):
            row, col = divmod(i, 2)

            axs[row, col].scatter(
                df["air_temperature_c"],
                df[metric],
                alpha=0.5,
                label=metric.upper(),
                color=color,
            )

            valid_data = df.dropna(subset=["air_temperature_c", metric])
            x = valid_data["air_temperature_c"]
            y = valid_data[metric]
            if len(x) > 1:
                slope, intercept = np.polyfit(x, y, 1)
                regression_line = slope * x + intercept
                axs[row, col].plot(
                    x, regression_line, color="red", linewidth=2, label="Regression Line"
                )

            axs[row, col].set_xlabel("Temperature (°C)", color="white")
            axs[row, col].set_ylabel(f"{metric.upper()} (µg/m³)", color="white")
            axs[row, col].set_title(f"Temperature vs {metric.upper()}", color="white")
            axs[row, col].tick_params(colors="white")
            axs[row, col].legend(facecolor="none", edgecolor="none", labelcolor="white")
            axs[row, col].set_facecolor("none")

        axs[2, 1].axis("off")
        axs[2, 1].set_facecolor("none")

        st.pyplot(fig)

        # Plot 2: Line Plot of Temperature and Air Pollution Over Time (Aggregated by Year)
        st.subheader(f"Plot 2: Line Plot of Temperature and Air Pollution Over Time")
        df["year"] = df["date"].dt.year
        yearly_aggregated = df.groupby("year").mean().reset_index()

        fig, axes = plt.subplots(3, 2, figsize=(26, 18), facecolor="none")  # 3x2 grid (6 plots total)
        axes = axes.flatten()  # Flatten axes for easier indexing


        # Plot each metric along with temperature in the grid
        for idx, (metric, color) in enumerate(air_pollution_metrics):
            ax = axes[idx]  # Get the subplot axis

            # Plot temperature
            ax.plot(
                yearly_aggregated["year"],
                yearly_aggregated["air_temperature_c"],
                label="Temperature (°C)",
                color="red",
                linewidth=2,
            )

            # Plot the air pollution metric
            ax.plot(
                yearly_aggregated["year"],
                yearly_aggregated[metric],
                label=metric.upper(),
                color=color,
                alpha=0.7,
                linestyle="--",
            )

            # Customize the subplot
            ax.set_xlabel("Year", color="white")
            ax.set_ylabel("Concentration / Temperature", color="white")
            ax.set_title(f"Yearly Temperature and {metric.upper()}", color="white")
            ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x)}"))
            ax.tick_params(colors="white")
            ax.set_facecolor("none")

        # Hide the last subplot if there are fewer than 6 metrics
        if len(air_pollution_metrics) < len(axes):
            axes[-1].axis("off")

        # Adjust the layout
        fig.tight_layout()
        fig.patch.set_alpha(0.0)

        # Display the grid
        st.pyplot(fig)

        # Plot 3: Correlation Matrix Heatmap
        st.subheader(f"Plot 3: Temperature and Airpollution Correlation Matrix Heatmap")
        x_features = ["air_temperature_c", "max_temperature_c", "min_temperature_c"]
        x_aliases = ["Average Temperature", "Max Temperature", "Min Temperature"]
        y_features = ["so2", "no2", "pm25", "pm10", "o3"]
        selected_features = x_features + y_features

        filtered_df = df[selected_features].dropna()
        corr_matrix = filtered_df.corr().loc[y_features, x_features]

        fig, ax = plt.subplots(figsize=(10, 6), facecolor="none")

        cax = ax.matshow(corr_matrix, cmap="coolwarm", aspect="auto")
        colorbar = fig.colorbar(cax)
        colorbar.ax.yaxis.set_tick_params(color="white")
        colorbar.ax.yaxis.set_tick_params(labelcolor="white")

        ax.set_xticks(np.arange(len(x_features)))
        ax.set_yticks(np.arange(len(y_features)))
        ax.set_xticklabels(x_aliases, rotation=45, ha="right", color="white")
        ax.set_yticklabels(y_features, color="white")

        for (i, j), val in np.ndenumerate(corr_matrix.values):
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", color="white")

        ax.set_title("Correlation Matrix", color="white")
        ax.tick_params(colors="white")
        fig.patch.set_alpha(0.0)
        ax.set_facecolor("none")

        st.pyplot(fig)

    st.title("Q2: Does wind speed influence air pollution levels in Tallinn, and if so, how?")

    query_air_pollution_wind = f"""
    SELECT 
        d.year, d.month, d.day,
        o.avg_wind_speed_m_s,
        o.max_wind_speed_m_s,
        o.wind_direction_deg,
        o.so2_ug_m3, o.no2_ug_m3, o.pm25_ug_m3, o.pm10_ug_m3, o.o3_ug_m3
    FROM Observation AS o
    JOIN Date AS d ON o.date_id = d.id
    WHERE d.year IN ({','.join(map(str, selected_years))})
    AND o.avg_wind_speed_m_s BETWEEN {selected_wind_speed[0]} AND {selected_wind_speed[1]}
    ORDER BY d.year, d.month, d.day;
    """

    data_air_pollution_wind = con.execute(query_air_pollution_wind).fetchall()

    if data_air_pollution_wind:
        import pandas as pd
        df = pd.DataFrame(
            data_air_pollution_wind, 
            columns=["year", "month", "day", "avg_wind_speed_m_s", "max_wind_speed_m_s", "wind_direction_deg", "so2", "no2", "pm25", "pm10", "o3"]
        )
        yearly_aggregated = df.groupby("year").mean().reset_index()

        # Plot 1: Wind Speed vs Air Pollution Metrics
        st.subheader(f"Plot 1: Scatter Plot of Wind Speed vs Air Pollution Metrics")

        fig, axes = plt.subplots(3, 2, figsize=(26, 18), facecolor="none")
        axes = axes.flatten()

        for idx, (metric, color) in enumerate(air_pollution_metrics):
            if idx >= len(axes):
                break
            ax = axes[idx]
            
            ax.scatter(df["avg_wind_speed_m_s"], df[metric], alpha=0.5, label=f"{metric.upper()}", color=color)
            
            valid_data = df.dropna(subset=["avg_wind_speed_m_s", metric])
            x = valid_data["avg_wind_speed_m_s"]
            y = valid_data[metric]
            if len(x) > 1:
                slope, intercept = np.polyfit(x, y, 1)
                regression_line = slope * x + intercept
                ax.plot(x, regression_line, color="red", linewidth=2, label="Regression Line")
            
            ax.set_xlabel("Average Wind Speed (m/s)", color="white")
            ax.set_ylabel(f"{metric.upper()} (µg/m³)", color="white")
            ax.set_title(f"Wind Speed vs {metric.upper()}", color="white")
            ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
            ax.tick_params(colors="white")
            ax.set_facecolor("none")

        if len(air_pollution_metrics) < len(axes):
            for i in range(len(air_pollution_metrics), len(axes)):
                axes[i].axis("off")

        fig.tight_layout()
        fig.patch.set_alpha(0.0)
        st.pyplot(fig)

        # Plot 2: Yearly Wind Speed and Air Pollution Metrics
        st.subheader(f"Plot 2: Line Plot of Wind Speed and Air Pollution Over Time")
        fig, axes = plt.subplots(3, 2, figsize=(26, 18), facecolor="none")
        axes = axes.flatten()

        for idx, (metric, color) in enumerate(air_pollution_metrics):
            if idx >= len(axes):
                break
            ax = axes[idx]
            
            ax.plot(
                yearly_aggregated["year"],
                yearly_aggregated["avg_wind_speed_m_s"],
                label="Average Wind Speed (m/s)",
                color="red",
                linewidth=2,
            )
            ax.plot(
                yearly_aggregated["year"],
                yearly_aggregated[metric],
                label=f"{metric.upper()} (µg/m³)",
                color=color,
                alpha=0.7,
                linestyle="--",
            )
            
            ax.set_xlabel("Year", color="white")
            ax.set_ylabel("Concentration / Wind Speed", color="white")
            ax.set_title(f"Yearly Wind Speed and {metric.upper()}", color="white")
            ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x)}"))
            ax.tick_params(colors="white")
            ax.set_facecolor("none")

        if len(air_pollution_metrics) < len(axes):
            for i in range(len(air_pollution_metrics), len(axes)):
                axes[i].axis("off")

        fig.tight_layout()
        fig.patch.set_alpha(0.0)
        st.pyplot(fig)

        # Plot 3: Correlation Matrix Heatmap
        st.subheader(f"Plot 3: Temperature and Airpollution Correlation Matrix Heatmap")
        x_features = ["avg_wind_speed_m_s", "max_wind_speed_m_s", "wind_direction_deg"]
        x_aliases = ["Average Wind Speed", "Max Wind Speed", "Wind Direction"]
        y_features = ["so2", "no2", "pm25", "pm10", "o3"]
        selected_features = x_features + y_features

        filtered_df = df[selected_features].dropna()
        corr_matrix = filtered_df.corr().loc[y_features, x_features]

        fig, ax = plt.subplots(figsize=(10, 6), facecolor="none")

        cax = ax.matshow(corr_matrix, cmap="coolwarm", aspect="auto")
        colorbar = fig.colorbar(cax)
        colorbar.ax.yaxis.set_tick_params(color="white")
        colorbar.ax.yaxis.set_tick_params(labelcolor="white")

        ax.set_xticks(np.arange(len(x_features)))
        ax.set_yticks(np.arange(len(y_features)))
        ax.set_xticklabels(x_aliases, rotation=45, ha="right", color="white")
        ax.set_yticklabels(y_features, color="white")

        for (i, j), val in np.ndenumerate(corr_matrix.values):
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", color="white")

        ax.set_title("Correlation Matrix", color="white")
        ax.tick_params(colors="white")
        fig.patch.set_alpha(0.0)
        ax.set_facecolor("none")

        st.pyplot(fig)

        # Plot 4: Air Direction radar map against Airpollution
        st.subheader(f"Plot 4: Air Direction radar map against Airpollution")
        bins = np.arange(0, 361, 45)
        bin_labels = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

        df["wind_direction_bin"] = pd.cut(df["wind_direction_deg"], bins=bins, labels=bin_labels, right=False)

        wind_dir_avg = df.groupby("wind_direction_bin")[["so2", "no2", "pm25", "pm10", "o3"]].mean().reindex(bin_labels)

        angles = np.linspace(0, 2 * np.pi, len(bin_labels), endpoint=False).tolist()
        angles += angles[:1]

        pollution_metrics = ["so2", "no2", "pm25", "pm10", "o3"]
        plot_data = {metric: wind_dir_avg[metric].tolist() + [wind_dir_avg[metric].tolist()[0]] for metric in pollution_metrics}

        st.subheader("Radar Plots: Air Pollution Metrics by Wind Direction")
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8), subplot_kw={"projection": "polar"}, facecolor="none")

        for ax in [ax1, ax2]:
            ax.set_theta_zero_location("N")
            ax.set_theta_direction(-1)
            ax.set_xticks(angles[:-1])
            ax.set_xticklabels(bin_labels, color="white", fontsize=10)
            ax.yaxis.set_tick_params(labelcolor="white")
            ax.set_facecolor("none")

        ax1.plot(angles, plot_data["o3"], label="O3 (µg/m³)", color="cyan", linewidth=2, alpha=0.7)
        ax1.fill(angles, plot_data["o3"], color="cyan", alpha=0.1)
        ax1.set_title("Ozone (O3) by Wind Direction", color="white", fontsize=14)

        for metric, color in zip(["so2", "no2", "pm25", "pm10"], ["blue", "green", "purple", "orange"]):
            ax2.plot(angles, plot_data[metric], label=metric.upper(), color=color, linewidth=2, alpha=0.7)
            ax2.fill(angles, plot_data[metric], color=color, alpha=0.1)

        ax2.set_title("Other Metrics (SO2, NO2, PM25, PM10) by Wind Direction", color="white", fontsize=14)
        ax2.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), facecolor="none", edgecolor="none", labelcolor="white")

        fig.patch.set_alpha(0.0)
        st.pyplot(fig)

    st.title("Q3: On which days of the week, months, and seasons do meteorological features have the greatest and least impact on air pollution in Tallinn?")

    query = f"""
    SELECT 
        d.season,
        d.month,
        d.holiday,
        d.workday,
        CAST(STRFTIME('%w', CAST(d.year || '-' || d.month || '-' || d.day AS DATE)) AS INTEGER) AS day_of_week,
        AVG(o.so2_ug_m3) AS avg_so2,
        AVG(o.no2_ug_m3) AS avg_no2,
        AVG(o.pm25_ug_m3) AS avg_pm25,
        AVG(o.pm10_ug_m3) AS avg_pm10,
        AVG(o.o3_ug_m3) AS avg_o3
    FROM Observation o
    JOIN Date d ON o.date_id = d.id
    WHERE d.year IN ({','.join(map(str, selected_years))})
    GROUP BY d.season, d.month, d.holiday, d.workday, day_of_week
    ORDER BY d.season, d.month, day_of_week;
    """

    data = con.execute(query).fetchall()
    columns = ["season", "month", "holiday", "workday", "day_of_week", "avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"]
    df = pd.DataFrame(data, columns=columns)

    day_labels = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    df["day_of_week"] = df["day_of_week"].apply(lambda x: day_labels[int(x)])

    # Plot 1: Average Pollution Metrics by Month
    st.subheader(f"Plot 1: Average Pollution Metrics by Month")
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    monthly_data = df.groupby("month")[["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"]].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="none")
    for metric, color in zip(["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"], ["blue", "green", "purple", "orange", "cyan"]):
        ax.plot(monthly_data["month"], monthly_data[metric], label=metric.split('_')[1].upper(), color=color, linewidth=2)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(months, rotation=45, ha="right", color="white")
    ax.set_xlabel("Month", color="white")
    ax.set_ylabel("Concentration (µg/m³)", color="white")
    ax.set_title("Average Pollution Metrics by Month", color="white")
    ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
    ax.tick_params(colors="white")
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")
    st.pyplot(fig)

    # Plot 2: Average Pollution Metrics by Day of the Week
    st.subheader("Plot 2: Average Pollution Metrics by Day of the Week")
    weekly_data = df.groupby("day_of_week")[["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"]].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="none")
    for metric, color in zip(["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"], ["blue", "green", "purple", "orange", "cyan"]):
        ax.plot(weekly_data["day_of_week"], weekly_data[metric], label=metric.split('_')[1].upper(), color=color, linewidth=2)
    ax.set_xticks(range(7))
    ax.set_xticklabels(day_labels, rotation=45, ha="right", color="white")
    ax.set_xlabel("Day of the Week", color="white")
    ax.set_ylabel("Concentration (µg/m³)", color="white")
    ax.set_title("Average Pollution Metrics by Day of the Week", color="white")
    ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
    ax.tick_params(colors="white")
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")
    st.pyplot(fig)

    # Plot 3: Average Pollution Metrics by Season
    st.subheader("Plot 3: Average Pollution Metrics by Season")
    season_data = df.groupby("season")[["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"]].mean().reset_index()

    season_order = ["Winter", "Spring", "Summer", "Autumn"]
    season_data["season"] = pd.Categorical(season_data["season"], categories=season_order, ordered=True)
    season_data = season_data.sort_values("season")

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="none")
    for metric, color in zip(["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"], ["blue", "green", "purple", "orange", "cyan"]):
        ax.plot(season_data["season"], season_data[metric], label=metric.split('_')[1].upper(), color=color, linewidth=2)
    ax.set_xticks(range(len(season_order)))
    ax.set_xticklabels(season_order, rotation=45, ha="right", color="white")
    ax.set_xlabel("Season", color="white")
    ax.set_ylabel("Concentration (µg/m³)", color="white")
    ax.set_title("Average Pollution Metrics by Season", color="white")
    ax.legend(facecolor="none", edgecolor="none", labelcolor="white")
    ax.tick_params(colors="white")
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")
    st.pyplot(fig)

    # Plot 4: Average Pollution Metrics by Day Type (Workdays vs. Holidays + Week ends)
    st.subheader("Plot 4: Correlation Plot of Day Type and Pollution Metrics")

    df["day_type"] = df.apply(
        lambda row: "Holiday" if row["holiday"] == 1 else ("Weekend" if row["workday"] == 0 else "Workday"),
        axis=1
    )

    day_type_encoded = pd.get_dummies(df["day_type"], prefix="DayType")

    pollution_metrics = ["avg_so2", "avg_no2", "avg_pm25", "avg_pm10", "avg_o3"]
    combined_df = pd.concat([day_type_encoded, df[pollution_metrics]], axis=1)

    day_type_labels = {"DayType_Holiday": "Holiday", "DayType_Weekend": "Weekend", "DayType_Workday": "Workday"}

    correlation_matrix = combined_df.corr().loc[pollution_metrics, [col for col in day_type_encoded.columns]]
    correlation_matrix = correlation_matrix.rename(columns=day_type_labels, index=lambda x: x.split('_')[1].upper())

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="none")

    cax = ax.matshow(correlation_matrix, cmap="coolwarm", aspect="auto")
    colorbar = fig.colorbar(cax)
    colorbar.ax.yaxis.set_tick_params(color="white")
    colorbar.ax.yaxis.set_tick_params(labelcolor="white")

    ax.set_xticks(range(len(correlation_matrix.columns)))
    ax.set_yticks(range(len(correlation_matrix.index)))
    ax.set_xticklabels(correlation_matrix.columns, rotation=45, ha="right", color="white")
    ax.set_yticklabels(correlation_matrix.index, color="white")

    for (i, j), val in np.ndenumerate(correlation_matrix.values):
        ax.text(j, i, f"{val:.2f}", ha="center", va="center", color="white")

    ax.set_title("Correlation Between Day Types and Pollution Metrics", color="white", fontsize=14)
    ax.tick_params(colors="white")
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")

    st.pyplot(fig)