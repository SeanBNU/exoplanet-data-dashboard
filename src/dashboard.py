import panel as pn
import hvplot.pandas
import pandas as pd
from database import ExoplanetsDB

pn.extension()
pn.config.theme = 'dark'

class ExoplanetDashboard:
    def __init__(self):
        self.db = ExoplanetsDB("exoplanets.db")
        self.db.connect()
        
        self.df = self.db.get_all_exoplanets()
        self.discovery_df = self.db.get_discovery_methods_by_year()
        self.stats = self.db.get_basic_stats()
        self.filtered_df = self.df.copy()

    def create_mass_period_plot(self, data=None):
        data = self.filtered_df if data is None else data
        plot = data.hvplot.scatter(
            'pl_orbper', 'pl_bmasse',
            title='Planet Mass vs Orbital Period',
            xlabel='Orbital Period (days)',
            ylabel='Planet Mass (Earth masses)',
            hover_cols=['pl_name', 'hostname'],
            height=300, width=450
        )
        return pn.pane.HoloViews(plot)

    def create_mass_radius_plot(self, data=None):
        data = self.filtered_df if data is None else data
        plot = data.hvplot.scatter(
            'pl_bmasse', 'pl_rade',
            title='Mass-Radius Relationship',
            xlabel='Planet Mass (Earth masses)',
            ylabel='Planet Radius (Earth radii)',
            hover_cols=['pl_name', 'hostname'],
            height=300, width=450
        )
        return pn.pane.HoloViews(plot)

    def create_discovery_timeline(self):
        plot = self.discovery_df.hvplot.bar(
            'disc_year', 'count',
            by='discoverymethod',
            stacked=True,
            title='Exoplanet Discoveries by Method',
            xlabel='Year',
            ylabel='Number of Discoveries',
            height=300, width=450,
            legend='bottom',
            legend_cols=3,
            legend_opts={'click_policy': 'hide'},
            fontscale=0.8
        )
        return pn.pane.HoloViews(plot)

    def create_stats_panel(self):
        stats_md = f"""
        ### Dataset Overview
        **Total Planets:** {len(self.filtered_df)}
        **Average Planet Mass:** {self.filtered_df['pl_bmasse'].mean():.2f} Earth masses
        **Average Planet Radius:** {self.filtered_df['pl_rade'].mean():.2f} Earth radii
        **Unique Host Stars:** {self.filtered_df['hostname'].nunique()}
        """
        return pn.pane.Markdown(stats_md)

    def create_data_table(self, data=None):
        data = self.filtered_df if data is None else data
        display_cols = ['pl_name', 'hostname', 'pl_bmasse', 'pl_rade', 
                       'pl_orbper', 'discoverymethod', 'disc_year']
        
        return pn.widgets.Tabulator(
            data[display_cols],
            pagination='remote',
            page_size=10,
            sizing_mode='stretch_width',
            height=300,
            configuration={"columnDefaults": {"sortable": True, "filterable": True}}
        )

    def create_dashboard(self):
        header = pn.pane.Markdown('# Exoplanet Explorer Dashboard')
        mass_range = pn.widgets.RangeSlider(
            name='Planet Mass Range (Earth masses)',
            start=0,
            end=self.df['pl_bmasse'].max(),
            value=(0, self.df['pl_bmasse'].max()),
            step=1
        )

        scatter_panel = pn.Column(self.create_mass_period_plot())
        mass_radius_panel = pn.Column(self.create_mass_radius_plot())
        discovery_panel = pn.Column(self.create_discovery_timeline())
        stats_panel = pn.Column(self.create_stats_panel())
        table_panel = pn.Column(self.create_data_table())

        def update_dashboard(event):
            min_mass, max_mass = event.new
            self.filtered_df = self.df[
                (self.df['pl_bmasse'] >= min_mass) & 
                (self.df['pl_bmasse'] <= max_mass)
            ]
            
            scatter_panel[0] = self.create_mass_period_plot()
            mass_radius_panel[0] = self.create_mass_radius_plot()
            stats_panel[0] = self.create_stats_panel()
            table_panel[0] = self.create_data_table()

        mass_range.param.watch(update_dashboard, 'value')

        return pn.Column(
            header,
            pn.Row(mass_range),
            pn.Row(scatter_panel, mass_radius_panel, discovery_panel),
            pn.Row(stats_panel),
            pn.Row(pn.pane.Markdown('### Detailed Data'), table_panel),
            sizing_mode='stretch_width'
        )

    def cleanup(self):
        self.db.close()

dashboard = ExoplanetDashboard()
app = dashboard.create_dashboard()
app.servable()