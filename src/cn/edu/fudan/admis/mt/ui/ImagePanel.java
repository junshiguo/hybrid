/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.fudan.admis.mt.ui;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import javax.swing.JPanel;

/**
 *
 * @author If
 */
public class ImagePanel extends JPanel{

    private BufferedImage image;

    public ImagePanel(String path) {
       try {                
          image = ImageIO.read(new File(path));
       } catch (IOException ex) {
            // handle exception...
       }
    }
    
    public void setImage(String path) {
       try {                
          image = ImageIO.read(new File(path));
          repaint();
       } catch (IOException ex) {
            // handle exception...
       }
    }
    
    @Override
    public void paint(Graphics g) {
        super.paint(g);
        g.drawImage(image, 0, 0, null);
    }

}
