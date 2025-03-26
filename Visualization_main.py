
import streamlit as st
import pandas as pd
import numpy as np
from pyecharts.commons.utils import JsCode
from pyecharts.charts import Bar, Line  # 关键修正点
from pyecharts import options as opts
from pyecharts.globals import ThemeType
import streamlit.components.v1 as components

# 必须作为第一个Streamlit命令
st.set_page_config(
    page_title="5G网络运营看板",
    page_icon="📶",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 缓存数据库连接（单例模式）
@st.cache_resource(show_spinner="🔄 初始化数据库连接...")
def get_db_connection():
    return st.connection("my_database")

# 缓存数据查询（带自动刷新）
@st.cache_data(show_spinner="📊 正在加载数据...", ttl=3600)
def load_data(_conn, query_name, query_sql):
    try:
        with st.spinner(f"正在加载 {query_name} 数据..."):
            df = _conn.query(query_sql)
            if df.empty:
                st.warning(f"{query_name} 数据为空")
            return df
    except Exception as e:
        st.error(f"加载 {query_name} 失败: {str(e)}")
        return pd.DataFrame()

# 查询定义字典
QUERY_DICT = {
    "base_df": """
    SELECT 
        SJGZQYMC AS 省份编码,
        DSJGZQYMC AS 地市编码,
        frequency_band AS 频段,
        COUNT(DISTINCT station_name) AS 5g基站数,
        COUNT(DISTINCT cell_name) AS 5g小区数
    FROM btsbase
    GROUP BY SJGZQYMC, DSJGZQYMC, frequency_band
    """,
    
    "traffic_df":"""
    SELECT
        -- 按天和区域分组
        DATE(k.开始时间) AS 日期,
        b.SJGZQYMC AS 省份编码,
        b.DSJGZQYMC AS 地市编码,
        b.frequency_band AS 频段,
        -- 日级聚合计算
        ROUND((SUM(k.R2032_001) + SUM(k.R2032_012)) / 1e9,2) AS 总流量_TB,  -- 日总流量（TB）
        ROUND(SUM(k.R2032_012) / 1e9,2) AS 下行流量_TB,                     -- 日下行流量（GB）
        ROUND(SUM(k.R2032_001) / 1e9,2) AS 上行流量_TB,                     -- 日上行流量（GB）
        ROUND(SUM(k.K1009_001) / 4000,2) AS VoNR语音话务量_千Erl,          -- 日语音话务量
        ROUND(SUM(k.K1009_002) / 4000,2) AS ViNR视频话务量_千Erl          -- 日视频话务量
    FROM 
        btsbase b
    INNER JOIN 
        kpibase k ON b.ID = k.ID
    GROUP BY 
        DATE(k.开始时间),  -- 按天分组
        b.SJGZQYMC,         -- 按区域分组
        b.DSJGZQYMC,
        b.frequency_band;
    """,
    
    "kpi_df": """
            SELECT
                DATE(k.开始时间) AS 日期,
                b.SJGZQYMC AS 省份编码,
                b.DSJGZQYMC AS 地市编码,
                b.frequency_band AS 频段,
        
                -- 无线接通率
                ROUND(
                    (SUM(k.R1001_012) / NULLIF(SUM(k.R1001_001), 0)) 
                    * (SUM(k.R1034_012) / NULLIF(SUM(k.R1034_001), 0)) 
                    * (SUM(k.R1039_002) / NULLIF(SUM(k.R1039_001), 0)) 
                    * 100, 
                    2
                ) AS 无线接通率,
                
                -- 无线掉线率
                ROUND(
                    100 * (SUM(k.R2004_003) - SUM(k.R2004_004)) 
                    / NULLIF(SUM(k.R2004_003) + SUM(k.R2004_006), 0), 
                    2
                ) AS 无线掉线率,
                
                -- 切换成功率
                ROUND(
                    100 * (
                        (SUM(k.R2007_002) + SUM(k.R2007_004) + SUM(k.R2006_004) + SUM(k.R2006_008) + SUM(k.R2005_004) + SUM(k.R2005_008)) 
                        / NULLIF(
                            SUM(k.R2007_001) + SUM(k.R2007_003) + SUM(k.R2006_001) + SUM(k.R2006_005) + SUM(k.R2005_001) + SUM(k.R2005_005), 
                            0
                        )
                    ), 
                    2
                ) AS 切换成功率,
                
                -- VONR无线接通率
                ROUND(
                    100 * (SUM(k.R1034_013) / NULLIF(SUM(k.R1034_002), 0)) 
                    * (SUM(k.R1001_018) + SUM(k.R1001_015)) 
                    / NULLIF(SUM(k.R1001_007) + SUM(k.R1001_004), 0), 
                    2
                ) AS VONR无线接通率,
                
                -- VONR无线掉线率
                ROUND(
                    100 * (SUM(k.R2035_003) - SUM(k.R2035_013)) 
                    / NULLIF(SUM(k.R2035_003) + SUM(k.R2035_026), 0), 
                    2
                ) AS VONR无线掉线率,
                
                -- VONR切换成功率
                ROUND(
                    100 * (
                        (
                            SUM(k.R2005_027) + SUM(k.R2005_031) + SUM(k.R2005_039) 
                            + SUM(k.R2006_025) + SUM(k.R2006_029) + SUM(k.R2006_037) 
                            + SUM(k.R2007_008) + SUM(k.R2007_010) + SUM(k.R2007_014)
                        ) 
                        / NULLIF(
                            SUM(k.R2005_024) + SUM(k.R2005_028) + SUM(k.R2005_036) 
                            + SUM(k.R2006_022) + SUM(k.R2006_026) + SUM(k.R2006_034) 
                            + SUM(k.R2007_007) + SUM(k.R2007_009) + SUM(k.R2007_013), 
                            0
                        )
                    ), 
                    2
                ) AS VONR切换成功率
            FROM 
                btsbase b
            INNER JOIN 
                kpibase k ON b.ID = k.ID
            GROUP BY DATE(k.开始时间), b.SJGZQYMC, b.DSJGZQYMC , b.frequency_band;
    """
}

def main():
    # 初始化连接
    conn = get_db_connection()
    
    # 加载数据
    data = {
        name: load_data(conn, name, sql)
        for name, sql in QUERY_DICT.items()
    }
    
        # ========== 侧边栏 ==========
    with st.sidebar:
        st.header("数据筛选条件")
        
        # 获取有效日期范围
        try:
            traffic_dates = pd.to_datetime(data['traffic_df']['日期'])
            min_date = traffic_dates.min().date()
            max_date = traffic_dates.max().date()
        except KeyError:
            min_date = max_date = pd.to_datetime('today').date()

        # 日期范围选择（关键修正点）
        selected_dates = st.date_input(
            "日期筛选",
            value=[min_date, max_date],
            min_value=min_date,
            max_value=max_date
        )
        
        # 处理单选日期情况
        if len(selected_dates) == 1:
            selected_dates = [selected_dates[0], selected_dates[0]]

        # 地市多选（使用原始数据）
        try:
            cities = data['traffic_df']['地市编码'].unique().tolist()
            selected_cities = st.multiselect(
                "地市筛选",
                options=cities,
                default=cities
            )
        except KeyError:
            cities = []
            selected_cities = []

   # ========== 数据过滤 ==========

    def filter_data(df_name, df):
        """支持不同数据集的差异化过滤"""
        try:
            # 日期过滤（使用已定义的selected_dates）
            start_date = selected_dates[0]
            end_date = selected_dates[-1]  # 兼容单选情况
            
            date_mask = (
                (pd.to_datetime(df['日期']) >= pd.to_datetime(start_date)) &
                (pd.to_datetime(df['日期']) <= pd.to_datetime(end_date))
            )
            
            # 数据集特定过滤
            if df_name == 'traffic_df':
                city_mask = df['地市编码'].isin(selected_cities)
                return df[date_mask & city_mask]
            elif df_name == 'kpi_df':
                city_mask = df['地市编码'].isin(selected_cities)
                return df[date_mask & city_mask]
            else:
                return df
        except KeyError as e:
            st.error(f"数据集 {df_name} 缺少必要字段: {str(e)}")
            return df

    # 应用过滤
    filtered_data = {
        'base_df': data['base_df'][
            data['base_df']['地市编码'].isin(selected_cities)
        ].copy(),
        'traffic_df': filter_data('traffic_df', data['traffic_df']),
        'kpi_df': filter_data('kpi_df', data['kpi_df'])
    }


    # 标题区
    st.title("📶 5G网络运营")
    st.caption("数据更新周期：每小时自动刷新 | 数据源：YD核心网管系统")
    
    # 关键指标卡
    col1, col2, col3 ,col4 = st.columns(4)
    with col1:   
        try:
            total = filtered_data['base_df']['5g基站数'].sum()
            # 添加黄冈市保护
            band41_query = filtered_data['base_df'].query("频段 == 'band41'")
            band41 = band41_query['5g基站数'].sum() if not band41_query.empty else 0
            ratio = (band41 / total) * 100 if total > 0 else 0
            st.metric("5G基站数", 
                    f"{total:,}",
                    delta=f"{(ratio - 50):+.1f}% vs 700M",
                    help="2.6GHz频段(band41)基站占比")
        except KeyError:
            st.error("缺少必要数据列")
        except ZeroDivisionError:
            st.warning("无小区数据")
    
    with col2:
        try:
            total = filtered_data['base_df']['5g小区数'].sum()
            band41 = filtered_data['base_df'].query("频段 == 'band41'")['5g小区数'].sum()
            ratio = (band41 / total) * 100 if total > 0 else 0
            st.metric("5G小区数", 
                    f"{filtered_data['base_df']['5g小区数'].sum():,}",
                    delta=f"{(ratio - 50):+.1f}% vs 700M",
                    help="2.6GHz频段(band41)小区占比")
        except KeyError:
            st.error("缺少必要数据列")
        except ZeroDivisionError:
            st.warning("无小区数据")
    
    with col3:
        try:
            # 第一步：按日期汇总总流量后计算日均
            daily_total = filtered_data['traffic_df'].groupby('日期')['总流量_TB'].sum()
            total_avg = round(daily_total.mean(), 2) if not daily_total.empty else 0.00

            # 第二步：筛选band41后按日期汇总，再计算日均
            band41_df = filtered_data['traffic_df'].query("频段 == 'band41'")
            daily_band41 = band41_df.groupby('日期')['总流量_TB'].sum() if not band41_df.empty else pd.Series([0])
            band41_avg = round(daily_band41.mean(), 2) if not daily_band41.empty else 0.00

            # 计算占比（带多重保护）
            ratio = 0.00
            if total_avg > 0 and not np.isnan(total_avg):
                ratio = round((band41_avg / total_avg) * 100, 2)
                
                # 设置对比基准值（可配置化）
                BENCHMARK = 50  # 700M基准值
                delta_value = ratio - BENCHMARK
                
                st.metric(
                    label="日均数据流量", 
                    value=f"{total_avg:,.2f} TB",
                    delta=f"{delta_value:+.2f}% vs 700M",
                    help=(
                        "2.6GHz频段(band41)日均数据流量占比计算逻辑：\n"
                        "1. 按日期汇总各频段总流量\n"
                        "2. 计算各频段日流量的平均值\n"
                        "3. band41日均值 / 全频段日均值 × 100%"
                    )
                )
            else:
                st.metric(
                    label="日均数据流量", 
                    value=f"{total_avg:,.2f} TB",
                    delta="N/A",
                    help="无有效数据计算占比"
                )

        except KeyError as e:
            st.error(f"关键数据列缺失: {str(e)}")
        except ZeroDivisionError:
            st.warning("全频段日均流量为零，无法计算占比")
        except Exception as e:
            st.error(f"计算过程发生异常: {str(e)}")

    with col4:
        try:
            # 第一步：按日期汇总总流量后计算日均
            daily_total = filtered_data['traffic_df'].groupby('日期')['VoNR语音话务量_千Erl'].sum()
            total_avg = round(daily_total.mean(), 2) if not daily_total.empty else 0.00

            # 第二步：筛选band41后按日期汇总，再计算日均
            band41_df = filtered_data['traffic_df'].query("频段 == 'band41'")
            daily_band41 = band41_df.groupby('日期')['VoNR语音话务量_千Erl'].sum() if not band41_df.empty else pd.Series([0])
            band41_avg = round(daily_band41.mean(), 2) if not daily_band41.empty else 0.00

            # 计算占比（带多重保护）
            ratio = 0.00
            if total_avg > 0 and not np.isnan(total_avg):
                ratio = round((band41_avg / total_avg) * 100, 2)
                
                # 设置对比基准值（可配置化）
                BENCHMARK = 50  # 700M基准值
                delta_value = ratio - BENCHMARK
                
                st.metric(
                    label="VONR业务流量", 
                    value=f"{total_avg:,.2f} 千Erl",
                    delta=f"{delta_value:+.2f}% vs 700M",
                    help=(
                        "2.6GHz频段(band41)日均数据流量占比计算逻辑：\n"
                        "1. 按日期汇总各频段总流量\n"
                        "2. 计算各频段日流量的平均值\n"
                        "3. band41日均值 / 全频段日均值 × 100%"
                    )
                )
            else:
                st.metric(
                    label="日均VONR业务流量", 
                    value=f"{total_avg:,.2f} 千Erl",
                    delta="N/A",
                    help="无有效数据计算占比"
                )

        except KeyError as e:
            st.error(f"关键数据列缺失: {str(e)}")
        except ZeroDivisionError:
            st.warning("全频段VONR业务流量为零，无法计算占比")
        except Exception as e:
            st.error(f"计算过程发生异常: {str(e)}")

    # 可视化标签页
    tab1, tab2, tab3 = st.tabs(["📡 基站价值", "📶 网络性能", "📊 业务诊断"])
    
    with tab1:
        col4, col5, col6 = st.columns(3)
        with col4:
            st.subheader("主设备基站")
            if not filtered_data['base_df'].empty:
                # 处理数据
                pivot_df = filtered_data['base_df'].pivot_table(
                    index='地市编码',
                    columns='频段',
                    values='5g基站数',
                    aggfunc='sum',
                    fill_value=0
                )
                
                # 生成图表
                bar = (
                    Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="100%"))
                    .add_xaxis(pivot_df.index.tolist())
                    .add_yaxis("2.6G(band41)", pivot_df.get('band41', pd.Series(0)).tolist())
                    .add_yaxis("700M(band28)", pivot_df.get('band28', pd.Series(0)).tolist())
                    .set_global_opts(
                        title_opts=opts.TitleOpts(title=""),
                        toolbox_opts=opts.ToolboxOpts(),
                        datazoom_opts=[opts.DataZoomOpts()],
                        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=30))
                    )
                    )                    
                components.html(bar.render_embed(), height=500)
            else:
                st.warning("无基站分布数据")
# ========== 区域流量图表 (col5) ==========
        with col5:
            st.subheader("数据业务流量")
            if not filtered_data['traffic_df'].empty:
                pivot_df = filtered_data['traffic_df'].pivot_table(
                    index='日期',
                    columns='频段',
                    values='总流量_TB',
                    aggfunc='sum',
                    fill_value=0
                ).round(2)

                line = (
                    Line(init_opts=opts.InitOpts(
                        theme=ThemeType.LIGHT,
                        width="100%",
                        animation_opts=opts.AnimationOpts(animation=False)
                    ))
                    .add_xaxis(pivot_df.index.tolist())
                    .add_yaxis(
                        series_name="2.6G(band41)",
                        y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                        linestyle_opts=opts.LineStyleOpts(width=1),
                        label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                        markpoint_opts=opts.MarkPointOpts(
                            data=[
                                opts.MarkPointItem(type_="max", symbol_size=20),
                                opts.MarkPointItem(type_="min", symbol_size=20)
                            ],
                        symbol="roundRect",
                        symbol_size=12,
                        label_opts=opts.LabelOpts(
                            formatter=lambda params: f"{params.name}\n{params.value:.2f}千Erl",
                            position="inside"
                            )
                        )
                    )
                    .add_yaxis(
                        series_name="700M(band28)",
                        y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                        linestyle_opts=opts.LineStyleOpts(width=1),
                        label_opts=opts.LabelOpts(is_show=False),
                        markpoint_opts=opts.MarkPointOpts(
                            data=[
                                opts.MarkPointItem(type_="max", symbol_size=20),
                                opts.MarkPointItem(type_="min", symbol_size=20)
                            ],
                            symbol="roundRect",
                            symbol_size=12,
                            label_opts=opts.LabelOpts(
                            formatter=lambda params: f"{params.name}\n{params.value:.2f}千Erl",
                            position="inside"
                                )
                        )
                    )
                    .set_global_opts(
                        xaxis_opts=opts.AxisOpts(
                            axislabel_opts=opts.LabelOpts(is_show=False),
                            boundary_gap=False,
                        ),
                        yaxis_opts=opts.AxisOpts(
                            is_show=False,  # 隐藏纵坐标
                            splitline_opts=opts.SplitLineOpts(is_show=False)
                        ),
                        tooltip_opts=opts.TooltipOpts(
                            trigger="axis" ),
                        datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                        legend_opts=opts.LegendOpts(
                            pos_top="0.4%",
                            item_width=25,
                            item_height=12,  # 统一高度
                            item_gap=10,     # 统一间距
                            padding=[5, 0],  # 上/下内边距
                            textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                        )
                    )
                )
                components.html(line.render_embed(), height=500)

        # ========== 区域VONR话务图表 (col6) ========== 
        with col6:
            st.subheader("VONR话务量")
            if not filtered_data['traffic_df'].empty:
                pivot_df = filtered_data['traffic_df'].pivot_table(
                    index='日期',
                    columns='频段',
                    values='VoNR语音话务量_千Erl',
                    aggfunc='sum',
                    fill_value=0
                ).round(2)

                line = (
                    Line(init_opts=opts.InitOpts(
                        theme=ThemeType.LIGHT,
                        width="100%",
                        animation_opts=opts.AnimationOpts(animation=False)
                    ))
                    .add_xaxis(pivot_df.index.tolist())
                    .add_yaxis(
                        series_name="2.6G(band41)",
                        y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                        linestyle_opts=opts.LineStyleOpts(width=1),
                        label_opts=opts.LabelOpts(is_show=False),
                        markpoint_opts=opts.MarkPointOpts(
                            data=[
                                opts.MarkPointItem(type_="max", symbol_size=20),
                                opts.MarkPointItem(type_="min", symbol_size=20)
                            ],
                            symbol="roundRect",
                            symbol_size=12,
                            label_opts=opts.LabelOpts(
                            formatter=lambda params: f"{params.name}\n{params.value:.2f}千Erl",
                            position="inside"
                            )
                        )
                    )
                    .add_yaxis(
                        series_name="700M(band28)",
                        y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                        linestyle_opts=opts.LineStyleOpts(width=1),
                        label_opts=opts.LabelOpts(is_show=False),
                        markpoint_opts=opts.MarkPointOpts(
                            data=[
                                opts.MarkPointItem(type_="max", symbol_size=20),
                                opts.MarkPointItem(type_="min", symbol_size=20)
                            ],
                            symbol="roundRect",
                            symbol_size=12,
                            label_opts=opts.LabelOpts(
                            formatter=lambda params: f"{params.name}\n{params.value:.2f}千Erl",
                            position="inside"
                            )
                        )
                    )
                    .set_global_opts(
                        xaxis_opts=opts.AxisOpts(
                            axislabel_opts=opts.LabelOpts(is_show=False),
                            boundary_gap=False
                        ),
                        yaxis_opts=opts.AxisOpts(is_show=False),
                        tooltip_opts=opts.TooltipOpts(
                            trigger="axis"
                        ),
                        datazoom_opts=[opts.DataZoomOpts()],  # 统一交互轴
                        legend_opts=opts.LegendOpts(
                            pos_top="0.4%",
                            item_width=25,
                            item_height=12,  # 统一高度
                            item_gap=10,     # 统一间距
                            padding=[5, 0],  # 上/下内边距
                            textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                        )
                    )
                )
                components.html(line.render_embed(), height=500)

# ========== 修改后的tab2代码块 ==========
    with tab2:
        # 第一行容器
        with st.container():
            row1_col1, row1_col2, row1_col3 = st.columns(3)
            
            with row1_col1:
                st.subheader("无线接通率")
                if not filtered_data['kpi_df'].empty:
                    pivot_df = filtered_data['kpi_df'].pivot_table(
                        index='日期',
                        columns='频段',
                        values='无线接通率',
                        aggfunc='mean',
                        fill_value=0
                    ).round(2)

                    line = (
                        Line(init_opts=opts.InitOpts(
                            theme=ThemeType.LIGHT,
                            width="100%",
                            animation_opts=opts.AnimationOpts(animation=False)
                        ))
                        .add_xaxis(pivot_df.index.tolist())
                        .add_yaxis(
                            series_name="2.6G(band41)",
                            y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                        )
                        .add_yaxis(
                            series_name="700M(band28)",
                            y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),
                            )
                        .set_global_opts(
                            xaxis_opts=opts.AxisOpts(
                                axislabel_opts=opts.LabelOpts(is_show=False),
                                boundary_gap=False,
                            ),
                            yaxis_opts=opts.AxisOpts(
                                is_show=False,  # 隐藏纵坐标
                                splitline_opts=opts.SplitLineOpts(is_show=False),
                                min_=90,    # 固定最小值
                                max_=100    # 固定最大值
                            ),
                            tooltip_opts=opts.TooltipOpts(
                                trigger="axis" ),
                            datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                            legend_opts=opts.LegendOpts(
                                pos_top="0.4%",
                                item_width=25,
                                item_height=12,  # 统一高度
                                item_gap=10,     # 统一间距
                                padding=[5, 0],  # 上/下内边距
                                textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                            )
                        )
                    )
                    components.html(line.render_embed(), height=500)

            with row1_col2:
                st.subheader("无线掉线率")
                if not filtered_data['kpi_df'].empty:
                    pivot_df = filtered_data['kpi_df'].pivot_table(
                        index='日期',
                        columns='频段',
                        values='无线掉线率',
                        aggfunc='mean',
                        fill_value=0
                    ).round(2)

                    line = (
                        Line(init_opts=opts.InitOpts(
                            theme=ThemeType.LIGHT,
                            width="100%",
                            animation_opts=opts.AnimationOpts(animation=False)
                        ))
                        .add_xaxis(pivot_df.index.tolist())
                        .add_yaxis(
                            series_name="2.6G(band41)",
                            y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                        )
                        .add_yaxis(
                            series_name="700M(band28)",
                            y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),
                            )
                        .set_global_opts(
                            xaxis_opts=opts.AxisOpts(
                                axislabel_opts=opts.LabelOpts(is_show=False),
                                boundary_gap=False,
                            ),
                            yaxis_opts=opts.AxisOpts(
                                is_show=False,  # 隐藏纵坐标
                                splitline_opts=opts.SplitLineOpts(is_show=False),
                            ),
                            tooltip_opts=opts.TooltipOpts(
                                trigger="axis" ),
                            datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                            legend_opts=opts.LegendOpts(
                                pos_top="0.4%",
                                item_width=25,
                                item_height=12,  # 统一高度
                                item_gap=10,     # 统一间距
                                padding=[5, 0],  # 上/下内边距
                                textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                            )
                        )
                    )
                    components.html(line.render_embed(), height=500)

            with row1_col3:
                st.subheader("切换成功率")
                if not filtered_data['kpi_df'].empty:
                    pivot_df = filtered_data['kpi_df'].pivot_table(
                        index='日期',
                        columns='频段',
                        values='切换成功率',
                        aggfunc='mean',
                        fill_value=0
                    ).round(2)

                    line = (
                        Line(init_opts=opts.InitOpts(
                            theme=ThemeType.LIGHT,
                            width="100%",
                            animation_opts=opts.AnimationOpts(animation=False)
                        ))
                        .add_xaxis(pivot_df.index.tolist())
                        .add_yaxis(
                            series_name="2.6G(band41)",
                            y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                        )
                        .add_yaxis(
                            series_name="700M(band28)",
                            y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                            linestyle_opts=opts.LineStyleOpts(width=1),
                            label_opts=opts.LabelOpts(is_show=False),
                            )
                        .set_global_opts(
                            xaxis_opts=opts.AxisOpts(
                                axislabel_opts=opts.LabelOpts(is_show=False),
                                boundary_gap=False,
                            ),
                            yaxis_opts=opts.AxisOpts(
                                is_show=False,  # 隐藏纵坐标
                                splitline_opts=opts.SplitLineOpts(is_show=False),
                                min_=90,    # 固定最小值
                                max_=100    # 固定最大值
                            ),
                            tooltip_opts=opts.TooltipOpts(
                                trigger="axis" ),
                            datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                            legend_opts=opts.LegendOpts(
                                pos_top="0.4%",
                                item_width=25,
                                item_height=12,  # 统一高度
                                item_gap=10,     # 统一间距
                                padding=[5, 0],  # 上/下内边距
                                textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                            )
                        )
                    )
                    components.html(line.render_embed(), height=500)

    # 第二行        
        with st.container():
            row2_col1, row2_col2, row2_col3 = st.columns(3)
            
            with row2_col1:
                    st.subheader("VONR无线接通率")
                    if not filtered_data['kpi_df'].empty:
                        pivot_df = filtered_data['kpi_df'].pivot_table(
                            index='日期',
                            columns='频段',
                            values='VONR无线接通率',
                            aggfunc='mean',
                            fill_value=0
                        ).round(2)

                        line = (
                            Line(init_opts=opts.InitOpts(
                                theme=ThemeType.LIGHT,
                                width="100%",
                                animation_opts=opts.AnimationOpts(animation=False)
                            ))
                            .add_xaxis(pivot_df.index.tolist())
                            .add_yaxis(
                                series_name="2.6G(band41)",
                                y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                            )
                            .add_yaxis(
                                series_name="700M(band28)",
                                y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),
                                )
                            .set_global_opts(
                                xaxis_opts=opts.AxisOpts(
                                    axislabel_opts=opts.LabelOpts(is_show=False),
                                    boundary_gap=False,
                                ),
                                yaxis_opts=opts.AxisOpts(
                                    is_show=False,  # 隐藏纵坐标
                                    splitline_opts=opts.SplitLineOpts(is_show=False),
                                    min_=90,    # 固定最小值
                                    max_=100    # 固定最大值
                                ),
                                tooltip_opts=opts.TooltipOpts(
                                    trigger="axis" ),
                                datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                                legend_opts=opts.LegendOpts(
                                    pos_top="0.4%",
                                    item_width=25,
                                    item_height=12,  # 统一高度
                                    item_gap=10,     # 统一间距
                                    padding=[5, 0],  # 上/下内边距
                                    textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                                )
                            )
                        )
                        components.html(line.render_embed(), height=500)
            
            with row2_col2:
                    st.subheader("VONR无线掉线率")
                    if not filtered_data['kpi_df'].empty:
                        pivot_df = filtered_data['kpi_df'].pivot_table(
                            index='日期',
                            columns='频段',
                            values='VONR无线掉线率',
                            aggfunc='mean',
                            fill_value=0
                        ).round(2)

                        line = (
                            Line(init_opts=opts.InitOpts(
                                theme=ThemeType.LIGHT,
                                width="100%",
                                animation_opts=opts.AnimationOpts(animation=False)
                            ))
                            .add_xaxis(pivot_df.index.tolist())
                            .add_yaxis(
                                series_name="2.6G(band41)",
                                y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                            )
                            .add_yaxis(
                                series_name="700M(band28)",
                                y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),
                                )
                            .set_global_opts(
                                xaxis_opts=opts.AxisOpts(
                                    axislabel_opts=opts.LabelOpts(is_show=False),
                                    boundary_gap=False,
                                ),
                                yaxis_opts=opts.AxisOpts(
                                    is_show=False,  # 隐藏纵坐标
                                    splitline_opts=opts.SplitLineOpts(is_show=False)
                                ),
                                tooltip_opts=opts.TooltipOpts(
                                    trigger="axis" ),
                                datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                                legend_opts=opts.LegendOpts(
                                    pos_top="0.4%",
                                    item_width=25,
                                    item_height=12,  # 统一高度
                                    item_gap=10,     # 统一间距
                                    padding=[5, 0],  # 上/下内边距
                                    textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                                )
                            )
                        )
                        components.html(line.render_embed(), height=500)
            
            with row2_col3:
                    st.subheader("VONR切换成功率")
                    if not filtered_data['kpi_df'].empty:
                        pivot_df = filtered_data['kpi_df'].pivot_table(
                            index='日期',
                            columns='频段',
                            values='VONR切换成功率',
                            aggfunc='mean',
                            fill_value=0
                        ).round(2)

                        line = (
                            Line(init_opts=opts.InitOpts(
                                theme=ThemeType.LIGHT,
                                width="100%",
                                animation_opts=opts.AnimationOpts(animation=False)
                            ))
                            .add_xaxis(pivot_df.index.tolist())
                            .add_yaxis(
                                series_name="2.6G(band41)",
                                y_axis=pivot_df.get('band41', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),  # 关闭数据标签
                            )
                            .add_yaxis(
                                series_name="700M(band28)",
                                y_axis=pivot_df.get('band28', pd.Series(0)).tolist(),
                                linestyle_opts=opts.LineStyleOpts(width=1),
                                label_opts=opts.LabelOpts(is_show=False),
                                )
                            .set_global_opts(
                                xaxis_opts=opts.AxisOpts(
                                    axislabel_opts=opts.LabelOpts(is_show=False),
                                    boundary_gap=False,
                                ),
                                yaxis_opts=opts.AxisOpts(
                                    is_show=False,  # 隐藏纵坐标
                                    splitline_opts=opts.SplitLineOpts(is_show=False),
                                    min_=90,    # 固定最小值
                                    max_=100    # 固定最大值
                                ),
                                tooltip_opts=opts.TooltipOpts(
                                    trigger="axis" ),
                                datazoom_opts=[opts.DataZoomOpts()],  # 与col4相同的可活动轴
                                legend_opts=opts.LegendOpts(
                                    pos_top="0.4%",
                                    item_width=25,
                                    item_height=12,  # 统一高度
                                    item_gap=10,     # 统一间距
                                    padding=[5, 0],  # 上/下内边距
                                    textstyle_opts=opts.TextStyleOpts(font_size=12)  # 统一字体大小
                                )
                            )
                        )
                        components.html(line.render_embed(), height=500)

if __name__ == "__main__":
    main()
