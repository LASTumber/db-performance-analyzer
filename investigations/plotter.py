# В файле investigations/plotter.py

import matplotlib.pyplot as plt
import os


class Plotter:
    def __init__(self, base_output_dir="plots"):
        self.base_output_dir = base_output_dir
        if not os.path.exists(self.base_output_dir):
            os.makedirs(self.base_output_dir)

        self.line_styles = ['-', '--', '-.', ':']
        self.markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'h']
        self.colors = plt.cm.get_cmap('tab10').colors

    def build_plot(self, x_data, y_data_dict, title, x_label, y_label, filename, sub_dir=None):
        output_dir = self.base_output_dir
        if sub_dir:
            output_dir = os.path.join(self.base_output_dir, sub_dir)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(12, 8))

        for i, (label, y_data) in enumerate(y_data_dict.items()):
            color = self.colors[i % len(self.colors)]
            linestyle = self.line_styles[i % len(self.line_styles)]

            marker = None
            if len(x_data) < 10:
                marker = self.markers[i % len(self.markers)]

            ax.plot(x_data, y_data, label=label, color=color, linestyle=linestyle, marker=marker)

        ax.set_title(title, fontsize=16)
        ax.set_xlabel(x_label, fontsize=12)
        ax.set_ylabel(y_label, fontsize=12)
        ax.legend(fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=10)

        svg_path = os.path.join(output_dir, f"{filename}.svg")
        png_path = os.path.join(output_dir, f"{filename}.png")

        try:
            fig.savefig(svg_path)
            fig.savefig(png_path, dpi=300)
            print(f"График сохранен в '{output_dir}'")
        except Exception as e:
            print(f"Не удалось сохранить график: {e}")

        plt.close(fig)